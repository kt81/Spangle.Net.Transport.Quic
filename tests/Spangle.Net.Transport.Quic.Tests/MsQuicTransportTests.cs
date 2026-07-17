using System.Net;
using System.Net.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using Spangle.Net.Transport.Quic.MsQuic;

namespace Spangle.Net.Transport.Quic.Tests;

/// <summary>
/// Exercises the real System.Net.Quic backend. QUIC needs native msquic and the dual-mode
/// sockets an IPv6 stack provides, so the loopback test runs only where
/// <see cref="MsQuicTransport.IsSupported"/> is true (e.g. an IPv6-enabled CI runner with
/// libmsquic); elsewhere it asserts the backend refuses cleanly instead of silently no-op'ing.
/// </summary>
public class MsQuicTransportTests
{
    private static readonly SslApplicationProtocol Moq = new("moq-echo");

    [SkippableFact]
    public void IsSupported_WhenRequiredByEnvironment_MustBeTrue()
    {
        // CI sets SPANGLE_REQUIRE_QUIC on jobs that must exercise the real backend
        // (e.g. Windows, where msquic is in-box). Without this, every job could silently
        // skip the loopback test and the suite would go green having never touched msquic.
        string? require = Environment.GetEnvironmentVariable("SPANGLE_REQUIRE_QUIC");
        Skip.If(!string.Equals(require, "1", StringComparison.Ordinal)
                && !string.Equals(require, "true", StringComparison.OrdinalIgnoreCase),
            "SPANGLE_REQUIRE_QUIC is not set; this canary only fires where the real backend is required.");

        MsQuicTransport.Shared.IsSupported.Should().BeTrue(
            "SPANGLE_REQUIRE_QUIC is set, so this platform must be able to run the real msquic backend");
    }

    [SkippableFact]
    public async Task Unsupported_Platform_ThrowsPlatformNotSupported()
    {
        Skip.If(MsQuicTransport.Shared.IsSupported, "covered by the loopback test on supported platforms");

        var options = new QuicServerOptions
        {
            ListenEndPoint = new IPEndPoint(IPAddress.Loopback, 0),
            ApplicationProtocols = [Moq],
        };
        Func<Task> act = async () => await MsQuicTransport.Shared.ListenAsync(options);
        await act.Should().ThrowAsync<PlatformNotSupportedException>();
    }

    [SkippableFact]
    public async Task Loopback_RoundTripsAStream_WhenSupported()
    {
        Skip.IfNot(MsQuicTransport.Shared.IsSupported,
            "no msquic / no IPv6 here; see Unsupported_Platform_ThrowsPlatformNotSupported");

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        CancellationToken ct = cts.Token;
        IQuicTransport transport = MsQuicTransport.Shared;

        using X509Certificate2 certificate = CreateSelfSignedCertificate();
        await using IQuicServer server = await transport.ListenAsync(new QuicServerOptions
        {
            ListenEndPoint = new IPEndPoint(IPAddress.Loopback, 0),
            ApplicationProtocols = [Moq],
            ServerCertificate = certificate,
        }, ct);

        ValueTask<IQuicConnection> acceptTask = server.AcceptConnectionAsync(ct);
        await using IQuicConnection client = await transport.ConnectAsync(new QuicClientOptions
        {
            RemoteEndPoint = server.LocalEndPoint,
            ApplicationProtocols = [Moq],
            TargetHost = "localhost",
            AllowUntrustedCertificates = true,
        }, ct);
        await using IQuicConnection accepted = await acceptTask;

        client.NegotiatedApplicationProtocol.Should().Be(Moq);

        byte[] payload = Encoding.UTF8.GetBytes("real quic roundtrip");
        await using IQuicStream send = await client.OpenStreamAsync(QuicStreamDirection.Unidirectional, ct);

        // QUIC surfaces the stream to the peer only once the opener writes, so write first
        await send.WriteAsync(payload, completeWrites: true, ct);
        await using IQuicStream recv = await accepted.AcceptStreamAsync(ct);

        var received = new List<byte>();
        var buffer = new byte[256];
        int read;
        while ((read = await recv.ReadAsync(buffer, ct)) != 0)
        {
            received.AddRange(buffer[..read]);
        }

        received.Should().Equal(payload);
    }

    [SkippableFact]
    public async Task Loopback_ClientAcceptsServerOpenedStream_WhenSupported()
    {
        Skip.IfNot(MsQuicTransport.Shared.IsSupported, "covered by the in-memory backend where QUIC cannot run");

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        CancellationToken ct = cts.Token;
        IQuicTransport transport = MsQuicTransport.Shared;

        using X509Certificate2 certificate = CreateSelfSignedCertificate();
        await using IQuicServer server = await transport.ListenAsync(new QuicServerOptions
        {
            ListenEndPoint = new IPEndPoint(IPAddress.Loopback, 0),
            ApplicationProtocols = [Moq],
            ServerCertificate = certificate,
        }, ct);

        ValueTask<IQuicConnection> acceptTask = server.AcceptConnectionAsync(ct);
        await using IQuicConnection client = await transport.ConnectAsync(new QuicClientOptions
        {
            RemoteEndPoint = server.LocalEndPoint,
            ApplicationProtocols = [Moq],
            TargetHost = "localhost",
            AllowUntrustedCertificates = true,
        }, ct);
        await using IQuicConnection accepted = await acceptTask;

        // The server opens the stream and the client accepts it — the direction that needs
        // the client's inbound-stream credit (QUIC defaults it to 0, which used to throw).
        byte[] payload = Encoding.UTF8.GetBytes("server to client");
        await using IQuicStream send = await accepted.OpenStreamAsync(QuicStreamDirection.Unidirectional, ct);
        await send.WriteAsync(payload, completeWrites: true, ct);
        await using IQuicStream recv = await client.AcceptStreamAsync(ct);

        var received = new List<byte>();
        var buffer = new byte[256];
        int read;
        while ((read = await recv.ReadAsync(buffer, ct)) != 0)
        {
            received.AddRange(buffer[..read]);
        }

        received.Should().Equal(payload);
    }

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

        // Export and reimport so the private key lives in a store Schannel/msquic can use
        // for server authentication on Windows. The ephemeral key from CreateSelfSigned is
        // otherwise unusable there and the server aborts the handshake, which the client
        // sees as a 'UserCanceled' TLS alert.
        byte[] pfx = ephemeral.Export(X509ContentType.Pfx);
        return X509CertificateLoader.LoadPkcs12(pfx, password: null,
            keyStorageFlags: X509KeyStorageFlags.Exportable);
    }
}
