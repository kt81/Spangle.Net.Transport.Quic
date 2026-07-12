using System.Net;
using System.Net.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using Spangle.Net.Transport.Quic;
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

    [Fact]
    public void IsSupported_WhenRequiredByEnvironment_MustBeTrue()
    {
        // CI sets SPANGLE_REQUIRE_QUIC on jobs that must exercise the real backend
        // (e.g. Windows, where msquic is in-box). Without this, every job could silently
        // skip the loopback test and the suite would go green having never touched msquic.
        string? require = Environment.GetEnvironmentVariable("SPANGLE_REQUIRE_QUIC");
        if (!string.Equals(require, "1", StringComparison.Ordinal)
            && !string.Equals(require, "true", StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        MsQuicTransport.Shared.IsSupported.Should().BeTrue(
            "SPANGLE_REQUIRE_QUIC is set, so this platform must be able to run the real msquic backend");
    }

    [Fact]
    public async Task Unsupported_Platform_ThrowsPlatformNotSupported()
    {
        if (MsQuicTransport.Shared.IsSupported)
        {
            return; // covered by the loopback test on supported platforms
        }

        var options = new QuicServerOptions
        {
            ListenEndPoint = new IPEndPoint(IPAddress.Loopback, 0),
            ApplicationProtocols = [Moq],
        };
        Func<Task> act = async () => await MsQuicTransport.Shared.ListenAsync(options);
        await act.Should().ThrowAsync<PlatformNotSupportedException>();
    }

    [Fact]
    public async Task Loopback_RoundTripsAStream_WhenSupported()
    {
        if (!MsQuicTransport.Shared.IsSupported)
        {
            return; // no msquic / no IPv6 here; see Unsupported_Platform_ThrowsPlatformNotSupported
        }

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
        ValueTask<IQuicStream> serverAccept = accepted.AcceptStreamAsync(ct);
        await using IQuicStream send = await client.OpenStreamAsync(QuicStreamDirection.Unidirectional, ct);
        await using IQuicStream recv = await serverAccept;

        await send.WriteAsync(payload, completeWrites: true, ct);

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
        return request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddYears(1));
    }
}
