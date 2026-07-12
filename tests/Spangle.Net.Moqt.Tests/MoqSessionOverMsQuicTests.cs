using System.Net;
using System.Net.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using Spangle.Net.Moqt;
using Spangle.Net.Moqt.Messages;
using Spangle.Net.Moqt.Wire;
using Spangle.Net.Transport.Quic;
using Spangle.Net.Transport.Quic.MsQuic;

namespace Spangle.Net.Moqt.Tests;

/// <summary>
/// The MOQT SETUP handshake over the real System.Net.Quic backend, so CI validates the
/// control-stream framing and Key-Value-Pair codec against native msquic — not only the
/// in-memory transport. Runs when <see cref="MsQuicTransport.IsSupported"/> is true (the
/// Windows CI job sets SPANGLE_REQUIRE_QUIC, so a skip there becomes a failure) and is a
/// no-op where QUIC cannot run.
/// </summary>
public class MoqSessionOverMsQuicTests
{
    private static readonly SslApplicationProtocol Alpn = new(MoqtConstants.Alpn);

    [Fact]
    public async Task Handshake_ExchangesSetupOptions_OverRealQuic()
    {
        if (!MsQuicTransport.Shared.IsSupported)
        {
            return; // no msquic / no IPv6 here; the in-memory MoqSessionTests cover the logic
        }

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        CancellationToken ct = cts.Token;
        IQuicTransport transport = MsQuicTransport.Shared;

        using X509Certificate2 certificate = CreateSelfSignedCertificate();
        await using IQuicServer server = await transport.ListenAsync(new QuicServerOptions
        {
            ListenEndPoint = new IPEndPoint(IPAddress.Loopback, 0),
            ApplicationProtocols = [Alpn],
            ServerCertificate = certificate,
        }, ct);

        ValueTask<IQuicConnection> acceptConnTask = server.AcceptConnectionAsync(ct);
        IQuicConnection clientConn = await transport.ConnectAsync(new QuicClientOptions
        {
            RemoteEndPoint = server.LocalEndPoint,
            ApplicationProtocols = [Alpn],
            TargetHost = "localhost",
            AllowUntrustedCertificates = true,
        }, ct);
        IQuicConnection serverConn = await acceptConnTask;

        clientConn.NegotiatedApplicationProtocol.Should().Be(Alpn);

        var clientSetup = new SetupMessage([MoqKeyValuePair.Varint(MoqSetupOption.MaxRequestUpdates, 64)]);
        var serverSetup = new SetupMessage(
            [MoqKeyValuePair.FromBytes(MoqSetupOption.MoqtImplementation, Encoding.UTF8.GetBytes("spangle"))]);

        Task<MoqSession> serverSessionTask = MoqSession.AcceptAsync(serverConn, serverSetup, ct);
        await using MoqSession clientSession = await MoqSession.ConnectAsync(clientConn, clientSetup, ct);
        await using MoqSession serverSession = await serverSessionTask;

        Encoding.UTF8.GetString(clientSession.RemoteSetup.Options.Single(
            o => o.Type == MoqSetupOption.MoqtImplementation).Bytes).Should().Be("spangle");
        serverSession.RemoteSetup.Options.Single(
            o => o.Type == MoqSetupOption.MaxRequestUpdates).VarintValue.Should().Be(64UL);
    }

    private static X509Certificate2 CreateSelfSignedCertificate()
    {
        using var rsa = RSA.Create(2048);
        var request = new CertificateRequest("CN=localhost", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(
            new X509EnhancedKeyUsageExtension([new Oid("1.3.6.1.5.5.7.3.1")], critical: false));
        var subjectAlternativeName = new SubjectAlternativeNameBuilder();
        subjectAlternativeName.AddDnsName("localhost");
        request.CertificateExtensions.Add(subjectAlternativeName.Build());

        using X509Certificate2 ephemeral = request.CreateSelfSigned(
            DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddYears(1));
        byte[] pfx = ephemeral.Export(X509ContentType.Pfx);
        return X509CertificateLoader.LoadPkcs12(pfx, password: null, keyStorageFlags: X509KeyStorageFlags.Exportable);
    }
}
