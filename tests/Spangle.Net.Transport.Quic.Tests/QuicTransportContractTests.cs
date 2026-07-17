using System.Net;
using System.Net.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Spangle.Net.Transport.Quic.InMemory;
using Spangle.Net.Transport.Quic.MsQuic;

namespace Spangle.Net.Transport.Quic.Tests;

/// <summary>
/// The behavioral contract both backends must honor, run against each of them. The in-memory
/// backend's whole value is standing in for msquic; a case that passes on one backend and not
/// the other is a bug in the abstraction — and exactly the kind that lets a hang-shaped
/// protocol bug stay green in tests and fire only in production. Everything here is about the
/// paths protocol code branches on: what a dial failure throws, what a dead connection does to
/// a blocked read, what an abort looks like from the other end.
/// </summary>
public abstract class QuicTransportContractTests
{
    private static readonly SslApplicationProtocol Alpn = new("contract-test");

    /// <summary>The backend under test; skips (xunit) where it cannot run.</summary>
    protected abstract IQuicTransport CreateTransport();

    /// <summary>Server options for one test listener (with a certificate where the backend needs one).</summary>
    protected abstract QuicServerOptions NewServerOptions(SslApplicationProtocol alpn);

    /// <summary>Client options to dial <paramref name="remote"/>.</summary>
    protected abstract QuicClientOptions NewClientOptions(EndPoint remote, SslApplicationProtocol alpn);

    [SkippableFact]
    public async Task Dial_WithNothingListening_ThrowsConnectionRefused()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        CancellationToken ct = cts.Token;
        IQuicTransport transport = CreateTransport();

        // Bind a listener to learn a free port, then close it and dial the corpse.
        EndPoint target;
        await using (IQuicServer server = await transport.ListenAsync(NewServerOptions(Alpn), ct))
        {
            target = server.LocalEndPoint;
        }

        Func<Task> act = async () => await transport.ConnectAsync(NewClientOptions(target, Alpn), ct);
        (await act.Should().ThrowAsync<QuicTransportException>())
            .Which.Error.Should().Be(QuicTransportError.ConnectionRefused);
    }

    [SkippableFact]
    public async Task StreamAbort_SurfacesToThePeerAsStreamAborted()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        CancellationToken ct = cts.Token;
        IQuicTransport transport = CreateTransport();
        await using IQuicServer server = await transport.ListenAsync(NewServerOptions(Alpn), ct);
        (IQuicConnection clientConn, IQuicConnection serverConn) = await ConnectPairAsync(transport, server, ct);
        await using (clientConn)
        await using (serverConn)
        {
            await using IQuicStream outbound = await clientConn.OpenStreamAsync(QuicStreamDirection.Unidirectional, ct);
            await outbound.WriteAsync(new byte[] { 1, 2, 3 }, completeWrites: false, ct);
            await using IQuicStream inbound = await serverConn.AcceptStreamAsync(ct);
            await ReadExactlyAsync(inbound, 3, ct);

            outbound.Abort(42);

            Func<Task> act = async () => await inbound.ReadAsync(new byte[16], ct);
            (await act.Should().ThrowAsync<QuicTransportException>())
                .Which.Error.Should().Be(QuicTransportError.StreamAborted);
        }
    }

    [SkippableFact]
    public async Task ConnectionClose_FailsABlockedRead()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        CancellationToken ct = cts.Token;
        IQuicTransport transport = CreateTransport();
        await using IQuicServer server = await transport.ListenAsync(NewServerOptions(Alpn), ct);
        (IQuicConnection clientConn, IQuicConnection serverConn) = await ConnectPairAsync(transport, server, ct);
        await using (clientConn)
        await using (serverConn)
        {
            await using IQuicStream outbound = await clientConn.OpenStreamAsync(QuicStreamDirection.Unidirectional, ct);
            await outbound.WriteAsync(new byte[] { 1 }, completeWrites: false, ct);
            await using IQuicStream inbound = await serverConn.AcceptStreamAsync(ct);
            await ReadExactlyAsync(inbound, 1, ct);

            // The read blocks — nothing more is coming — then the connection dies under it.
            // A blocked read hanging forever here is the divergence that used to let hang
            // bugs pass in-memory and fire on msquic.
            Task<int> blocked = inbound.ReadAsync(new byte[16], ct).AsTask();
            await Task.Delay(50, ct); // let the read actually park
            await clientConn.CloseAsync(0, ct);

            // `blocked` is this test's own read; awaiting it is not the foreign-task hazard
            // VSTHRD003 warns about.
#pragma warning disable VSTHRD003
            Func<Task> act = async () => await blocked;
#pragma warning restore VSTHRD003
            await act.Should().ThrowAsync<QuicTransportException>();
        }
    }

    [SkippableFact]
    public async Task ConnectionCloseByPeer_FailsABlockedAcceptStream()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        CancellationToken ct = cts.Token;
        IQuicTransport transport = CreateTransport();
        await using IQuicServer server = await transport.ListenAsync(NewServerOptions(Alpn), ct);
        (IQuicConnection clientConn, IQuicConnection serverConn) = await ConnectPairAsync(transport, server, ct);
        await using (clientConn)
        await using (serverConn)
        {
            ValueTask<IQuicStream> blocked = serverConn.AcceptStreamAsync(ct);
            await Task.Delay(50, ct);
            await clientConn.CloseAsync(0, ct);

            Func<Task> act = async () => await blocked;
            (await act.Should().ThrowAsync<QuicTransportException>())
                .Which.Error.Should().Be(QuicTransportError.ConnectionAborted);
        }
    }

    [SkippableFact]
    public async Task ServerDispose_UnblocksAPendingAcceptConnection()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        CancellationToken ct = cts.Token;
        IQuicTransport transport = CreateTransport();
        IQuicServer server = await transport.ListenAsync(NewServerOptions(Alpn), ct);

        ValueTask<IQuicConnection> blocked = server.AcceptConnectionAsync(ct);
        await Task.Delay(50, ct);
        await server.DisposeAsync();

        Func<Task> act = async () => await blocked;
        await act.Should().ThrowAsync<ObjectDisposedException>();
    }

    [SkippableFact]
    public async Task Cancellation_UnblocksAPendingAcceptStream()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        CancellationToken ct = cts.Token;
        IQuicTransport transport = CreateTransport();
        await using IQuicServer server = await transport.ListenAsync(NewServerOptions(Alpn), ct);
        (IQuicConnection clientConn, IQuicConnection serverConn) = await ConnectPairAsync(transport, server, ct);
        await using (clientConn)
        await using (serverConn)
        {
            using var acceptCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            ValueTask<IQuicStream> blocked = serverConn.AcceptStreamAsync(acceptCts.Token);
            await Task.Delay(50, ct);
            await acceptCts.CancelAsync();

            Func<Task> act = async () => await blocked;
            await act.Should().ThrowAsync<OperationCanceledException>();
        }
    }

    [SkippableFact]
    public async Task Cancellation_UnblocksAPendingRead()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        CancellationToken ct = cts.Token;
        IQuicTransport transport = CreateTransport();
        await using IQuicServer server = await transport.ListenAsync(NewServerOptions(Alpn), ct);
        (IQuicConnection clientConn, IQuicConnection serverConn) = await ConnectPairAsync(transport, server, ct);
        await using (clientConn)
        await using (serverConn)
        {
            await using IQuicStream outbound = await clientConn.OpenStreamAsync(QuicStreamDirection.Unidirectional, ct);
            await outbound.WriteAsync(new byte[] { 1 }, completeWrites: false, ct);
            await using IQuicStream inbound = await serverConn.AcceptStreamAsync(ct);
            await ReadExactlyAsync(inbound, 1, ct);

            using var readCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            Task<int> blocked = inbound.ReadAsync(new byte[16], readCts.Token).AsTask();
            await Task.Delay(50, ct);
            await readCts.CancelAsync();

            // `blocked` is this test's own read; awaiting it is not the foreign-task hazard
            // VSTHRD003 warns about.
#pragma warning disable VSTHRD003
            Func<Task> act = async () => await blocked;
#pragma warning restore VSTHRD003
            await act.Should().ThrowAsync<OperationCanceledException>();
        }
    }

    [SkippableFact]
    public async Task WriteAfterThePeerStopsReading_Throws()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        CancellationToken ct = cts.Token;
        IQuicTransport transport = CreateTransport();
        await using IQuicServer server = await transport.ListenAsync(NewServerOptions(Alpn), ct);
        (IQuicConnection clientConn, IQuicConnection serverConn) = await ConnectPairAsync(transport, server, ct);
        await using (clientConn)
        await using (serverConn)
        {
            await using IQuicStream outbound = await clientConn.OpenStreamAsync(QuicStreamDirection.Unidirectional, ct);
            await outbound.WriteAsync(new byte[] { 1 }, completeWrites: false, ct);
            await using IQuicStream inbound = await serverConn.AcceptStreamAsync(ct);
            await ReadExactlyAsync(inbound, 1, ct);

            // The receiver walks away mid-stream. The writer must eventually learn — a writer
            // that "succeeds" forever into the void is how a publisher keeps believing it is
            // delivering.
            inbound.Abort(7);

            var chunk = new byte[16 * 1024];
            Func<Task> act = async () =>
            {
                while (true)
                {
                    ct.ThrowIfCancellationRequested();
                    await outbound.WriteAsync(chunk, completeWrites: false, ct);
                }
            };
            await act.Should().ThrowAsync<QuicTransportException>();
        }
    }

    [SkippableFact]
    public async Task LocalConnectionDispose_FailsAPendingAcceptStreamAsDisposed()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        CancellationToken ct = cts.Token;
        IQuicTransport transport = CreateTransport();
        await using IQuicServer server = await transport.ListenAsync(NewServerOptions(Alpn), ct);
        (IQuicConnection clientConn, IQuicConnection serverConn) = await ConnectPairAsync(transport, server, ct);
        await using (clientConn)
        {
            ValueTask<IQuicStream> blocked = serverConn.AcceptStreamAsync(ct);
            await Task.Delay(50, ct);
            await serverConn.DisposeAsync();

            Func<Task> act = async () => await blocked;
            await act.Should().ThrowAsync<ObjectDisposedException>(
                "disposing the connection is using a disposed object, and msquic says so too");
        }
    }

    [SkippableFact]
    public async Task LocalConnectionClose_FailsAPendingAcceptStreamAsOperationAborted()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        CancellationToken ct = cts.Token;
        IQuicTransport transport = CreateTransport();
        await using IQuicServer server = await transport.ListenAsync(NewServerOptions(Alpn), ct);
        (IQuicConnection clientConn, IQuicConnection serverConn) = await ConnectPairAsync(transport, server, ct);
        await using (clientConn)
        await using (serverConn)
        {
            ValueTask<IQuicStream> blocked = serverConn.AcceptStreamAsync(ct);
            await Task.Delay(50, ct);
            await serverConn.CloseAsync(0, ct);

            Func<Task> act = async () => await blocked;
            (await act.Should().ThrowAsync<QuicTransportException>())
                .Which.Error.Should().Be(QuicTransportError.OperationAborted);
        }
    }

    private async Task<(IQuicConnection Client, IQuicConnection Server)> ConnectPairAsync(
        IQuicTransport transport, IQuicServer server, CancellationToken ct)
    {
        ValueTask<IQuicConnection> acceptTask = server.AcceptConnectionAsync(ct);
        IQuicConnection client = await transport.ConnectAsync(NewClientOptions(server.LocalEndPoint, Alpn), ct);
        IQuicConnection accepted = await acceptTask;
        return (client, accepted);
    }

    private static async Task ReadExactlyAsync(IQuicStream stream, int count, CancellationToken ct)
    {
        var buffer = new byte[count];
        var total = 0;
        while (total < count)
        {
            int read = await stream.ReadAsync(buffer.AsMemory(total), ct);
            read.Should().BePositive("the stream must not end before the expected bytes arrive");
            total += read;
        }
    }
}

/// <summary>The contract on the in-memory backend — it must behave like msquic to be worth anything.</summary>
public sealed class InMemoryQuicTransportContractTests : QuicTransportContractTests
{
    protected override IQuicTransport CreateTransport() => new InMemoryQuicTransport();

    protected override QuicServerOptions NewServerOptions(SslApplicationProtocol alpn) => new()
    {
        ListenEndPoint = new IPEndPoint(IPAddress.Loopback, 0),
        ApplicationProtocols = [alpn],
    };

    protected override QuicClientOptions NewClientOptions(EndPoint remote, SslApplicationProtocol alpn) => new()
    {
        RemoteEndPoint = remote,
        ApplicationProtocols = [alpn],
    };
}

/// <summary>The contract on real msquic, where the platform can run it.</summary>
public sealed class MsQuicTransportContractTests : QuicTransportContractTests
{
    // One certificate for the whole run: creating and re-importing one per test is the slow
    // part, and nothing here depends on certificate identity.
    private static readonly Lazy<X509Certificate2> s_certificate = new(CreateSelfSignedCertificate);

    protected override IQuicTransport CreateTransport()
    {
        Skip.IfNot(MsQuicTransport.Shared.IsSupported,
            "no msquic / no IPv6 here; the in-memory derivation covers the contract's logic");
        return MsQuicTransport.Shared;
    }

    protected override QuicServerOptions NewServerOptions(SslApplicationProtocol alpn) => new()
    {
        ListenEndPoint = new IPEndPoint(IPAddress.Loopback, 0),
        ApplicationProtocols = [alpn],
        ServerCertificate = s_certificate.Value,
    };

    protected override QuicClientOptions NewClientOptions(EndPoint remote, SslApplicationProtocol alpn) => new()
    {
        RemoteEndPoint = remote,
        ApplicationProtocols = [alpn],
        TargetHost = "localhost",
        AllowUntrustedCertificates = true,
    };

    private static X509Certificate2 CreateSelfSignedCertificate()
    {
        using var rsa = RSA.Create(2048);
        var request = new CertificateRequest("CN=localhost", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(
            new X509EnhancedKeyUsageExtension([new Oid("1.3.6.1.5.5.7.3.1")], critical: false)); // server auth
        var subjectAlternativeName = new SubjectAlternativeNameBuilder();
        subjectAlternativeName.AddDnsName("localhost");
        request.CertificateExtensions.Add(subjectAlternativeName.Build());

        using X509Certificate2 ephemeral = request.CreateSelfSigned(
            DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddYears(1));

        // Export and reimport so the private key lives in a store Schannel/msquic can use for
        // server authentication on Windows (see MsQuicTransportTests for the long version).
        byte[] pfx = ephemeral.Export(X509ContentType.Pfx);
        return X509CertificateLoader.LoadPkcs12(pfx, password: null,
            X509KeyStorageFlags.Exportable | X509KeyStorageFlags.PersistKeySet);
    }
}
