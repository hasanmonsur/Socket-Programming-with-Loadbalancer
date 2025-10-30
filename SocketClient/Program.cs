using System.Net.Sockets;
using System.Text;
using System.Diagnostics;

Console.WriteLine("🔹 Starting 100 parallel requests to Gateway...");

string host = "192.168.1.7";
int port = 9000;
int totalRequests = 100000;

var stopwatch = Stopwatch.StartNew();

await Parallel.ForEachAsync(Enumerable.Range(1, totalRequests), async (i, token) =>
{
    try
    {
        using var client = new TcpClient();
        await client.ConnectAsync(host, port, token);
        using var stream = client.GetStream();

        string message = $"Hello from client #{i}";
        var data = Encoding.UTF8.GetBytes(message);

        await stream.WriteAsync(data, token);

        var buffer = new byte[1024];
        int read = await stream.ReadAsync(buffer, token);

        string response = Encoding.UTF8.GetString(buffer, 0, read);
        Console.WriteLine($"✅ [{i}] Response: {response}");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"❌ [{i}] Error: {ex.Message}");
    }
});

stopwatch.Stop();
Console.WriteLine($"\n🏁 Completed {totalRequests} requests in {stopwatch.ElapsedMilliseconds} ms");
