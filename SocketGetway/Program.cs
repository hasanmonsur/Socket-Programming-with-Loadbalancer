using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Collections.Concurrent;

// -------------------- Config --------------------
var backendServers = new List<(string host, int port, int weight)>
{
    ("192.168.1.7", 9001, 5),
    ("192.168.1.7", 9002, 3),
    ("192.168.1.7", 9003, 2),
    ("192.168.1.7", 9004, 1)
};

const int gatewayPort = 9000;
var listener = new TcpListener(IPAddress.Any, gatewayPort);
Console.Title = $"⚙️ Persistent Gateway [.NET 8] — Port {gatewayPort}";
Console.WriteLine($"🚀 Gateway starting on port {gatewayPort}");
listener.Start();

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    Console.WriteLine("\n🛑 Shutting down gateway...");
    cts.Cancel();
};

// -------------------- Backend Pool --------------------

// Pool per backend
var backendPools = new ConcurrentDictionary<(string host, int port, int weight), ConcurrentQueue<TcpClient>>();
// Health status
var backendStatus = new ConcurrentDictionary<(string host, int port, int weight), bool>();

foreach (var backend in backendServers)
{
    backendPools[backend] = new ConcurrentQueue<TcpClient>();
    backendStatus[backend] = false; // initial health false
}

// Start background health monitor
_ = Task.Run(() => HealthMonitorAsync(backendServers, backendStatus, backendPools, cts.Token));

// -------------------- Accept Clients --------------------
try
{
    while (!cts.Token.IsCancellationRequested)
    {
        var client = await listener.AcceptTcpClientAsync(cts.Token);
        _ = Task.Run(() => HandleClientAsync(client, backendServers, backendPools, backendStatus, cts.Token));
    }
}
catch (OperationCanceledException)
{
    Console.WriteLine("✅ Gateway stopped gracefully.");
}
finally
{
    listener.Stop();
}

return;

// ====================== Methods ======================

static async Task HandleClientAsync(
    TcpClient client,
    List<(string host, int port, int weight)> backends,
    ConcurrentDictionary<(string host, int port, int weight), ConcurrentQueue<TcpClient>> pools,
    ConcurrentDictionary<(string host, int port, int weight), bool> status,
    CancellationToken token)
{
    var clientEndPoint = client.Client.RemoteEndPoint?.ToString() ?? "Unknown";
    Console.WriteLine($"🔗 Client connected: {clientEndPoint}");

    using var stream = client.GetStream();
    var buffer = new byte[4096];

    while (!token.IsCancellationRequested)
    {
        int read;
        try
        {
            read = await stream.ReadAsync(buffer.AsMemory(0, buffer.Length), token);
            if (read == 0) break;
        }
        catch
        {
            break;
        }

        var message = Encoding.UTF8.GetString(buffer, 0, read);
        Console.WriteLine($"📩 From {clientEndPoint}: {message}");

        var backend = GetWeightedHealthyBackend(backends, status);
        if (backend == default)
        {
            var msg = Encoding.UTF8.GetBytes("❌ No healthy backend available.\n");
            await stream.WriteAsync(msg, token);
            continue;
        }

        var backendClient = await GetBackendConnectionAsync(backend, pools, token);
        try
        {
            var backendStream = backendClient.GetStream();

            var data = Encoding.UTF8.GetBytes(message);
            await backendStream.WriteAsync(data, token);

            var responseBuffer = new byte[4096];
            int backendRead = await backendStream.ReadAsync(responseBuffer.AsMemory(0, responseBuffer.Length), token);
            await stream.WriteAsync(responseBuffer.AsMemory(0, backendRead), token);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"❌ Backend {backend.host}:{backend.port} error: {ex.Message}");
            backendClient.Close(); // drop bad connection
        }
        finally
        {
            if (backendClient.Connected)
                pools[backend].Enqueue(backendClient); // return to pool
        }
    }

    client.Close();
    Console.WriteLine($"❎ Client disconnected: {clientEndPoint}");
}

// ---------- Weighted Healthy Backend ----------
static (string host, int port, int weight) GetWeightedHealthyBackend(
    List<(string host, int port, int weight)> backends,
    ConcurrentDictionary<(string host, int port, int weight), bool> status)
{
    var healthy = backends.Where(b => status.GetValueOrDefault(b)).ToList();
    if (healthy.Count == 0) return default;

    // Randomly pick based on weight
    var weightedList = new List<(string host, int port, int weight)>();
    foreach (var b in healthy)
        for (int i = 0; i < b.weight; i++)
            weightedList.Add(b);

    var rnd = new Random();
    return weightedList[rnd.Next(weightedList.Count)];
}

// ---------- Get or Create Backend Connection ----------
static async Task<TcpClient> GetBackendConnectionAsync(
    (string host, int port, int weight) backend,
    ConcurrentDictionary<(string host, int port, int weight), ConcurrentQueue<TcpClient>> pools,
    CancellationToken token)
{
    var queue = pools[backend];
    if (queue.TryDequeue(out var client) && client.Connected)
        return client;

    // Create new TCP connection
    client = new TcpClient();
    await client.ConnectAsync(backend.host, backend.port, token);
    return client;
}

// ---------- Health Monitor ----------
static async Task HealthMonitorAsync(
    List<(string host, int port, int weight)> backends,
    ConcurrentDictionary<(string host, int port, int weight), bool> status,
    ConcurrentDictionary<(string host, int port, int weight), ConcurrentQueue<TcpClient>> pools,
    CancellationToken token)
{
    while (!token.IsCancellationRequested)
    {
        foreach (var backend in backends)
        {
            bool alive = await PingBackendAsync(backend.host, backend.port, token);
            status[backend] = alive;

            if (!alive)
            {
                // Close all connections in pool
                if (pools.TryGetValue(backend, out var q))
                {
                    while (q.TryDequeue(out var client))
                        client.Close();
                }
            }

            Console.WriteLine($"HealthCheck → {backend.host}:{backend.port} = {(alive ? "✅ UP" : "❌ DOWN")}");
        }

        await Task.Delay(5000, token); // check every 5 sec
    }
}

// ---------- Ping Backend ----------
static async Task<bool> PingBackendAsync(string host, int port, CancellationToken token)
{
    try
    {
        using var tcp = new TcpClient();
        var connectTask = tcp.ConnectAsync(host, port, token).AsTask();
        var timeout = Task.Delay(1000, token); // 1 s timeout
        var completed = await Task.WhenAny(connectTask, timeout);
        return completed == connectTask && tcp.Connected;

    }
    catch
    {
        return false;
    }
}
