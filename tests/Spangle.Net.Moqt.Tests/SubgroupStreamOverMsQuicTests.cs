using System.Net;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using Spangle.Net.Moqt;
using Spangle.Net.Moqt.Data;
using Spangle.Net.Transport.Quic;
using Spangle.Net.Transport.Quic.MsQuic;

namespace Spangle.Net.Moqt.Tests;

/// <summary>
/// The subgroup data plane over real msquic, so CI proves objects flow on a genuine QUIC
/// stream (its flow control, FIN semantics) — not only the in-memory pipe. Runs when QUIC is
/// supported; the Windows CI job requires it via SPANGLE_REQUIRE_QUIC.
/// </summary>
public class SubgroupStreamOverMsQuicTests
{
    private static readonly SslApplicationProtocol Alpn = new(MoqtConstants.Alpn);

    [Fact]
    public async Task SubgroupStream_RoundTripsObjects_OverRealQuic()
    {
        if (!MsQuicTransport.Shared.IsSupported)
        {
            return; // in-memory SubgroupStreamTests cover the logic where QUIC cannot run
        }

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        CancellationToken ct = cts.Token;
        IQuicTransport transport = MsQuicTransport.Shared;

        using X509Certificate2 certificate = TestCertificates.CreateSelfSigned();
        await using IQuicServer server = await transport.ListenAsync(new QuicServerOptions
        {
            ListenEndPoint = new IPEndPoint(IPAddress.Loopback, 0),
            ApplicationProtocols = [Alpn],
            ServerCertificate = certificate,
        }, ct);

        ValueTask<IQuicConnection> acceptTask = server.AcceptConnectionAsync(ct);
        await using IQuicConnection clientConn = await transport.ConnectAsync(new QuicClientOptions
        {
            RemoteEndPoint = server.LocalEndPoint,
            ApplicationProtocols = [Alpn],
            TargetHost = "localhost",
            AllowUntrustedCertificates = true,
        }, ct);
        await using IQuicConnection serverConn = await acceptTask;

        var header = new SubgroupHeader
        {
            TrackAlias = 42,
            GroupId = 1,
            SubgroupIdMode = SubgroupIdMode.Explicit,
            SubgroupId = 0,
            PublisherPriority = 64,
        };

        await using IQuicStream outbound = await clientConn.OpenStreamAsync(QuicStreamDirection.Unidirectional, ct);
        var writer = new SubgroupStreamWriter(outbound, header);
        for (ulong id = 0; id < 5; id++)
        {
            await writer.WriteObjectAsync(MoqObject.Normal(1, id, 0, 64, Encoding.UTF8.GetBytes($"object-{id}")), ct);
        }

        await writer.CompleteAsync(ct);

        await using IQuicStream inbound = await serverConn.AcceptStreamAsync(ct);
        var reader = await SubgroupStreamReader.OpenAsync(inbound, ct);
        reader.Header.TrackAlias.Should().Be(42UL);

        var payloads = new List<string>();
        while (await reader.ReadObjectAsync(ct) is { } moqObject)
        {
            payloads.Add(Encoding.UTF8.GetString(moqObject.Payload.Span));
        }

        payloads.Should().Equal("object-0", "object-1", "object-2", "object-3", "object-4");
    }
}
