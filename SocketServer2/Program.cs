using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;

Console.WriteLine("Welcome to Socket Server 1");

// -------------------------------
// ⚙️ Configuration
// -------------------------------
int port = args.Length > 0 ? int.Parse(args[0]) : 9001;
var listener = new TcpListener(IPAddress.Any, port);

Console.Title = $"Backend Service [{port}]";
Console.WriteLine($"✅ Backend listening on port {port}");

// -------------------------------
// 🔄 Graceful shutdown support
// -------------------------------
using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
    Console.WriteLine("\n⏹ Stopping backend listener...");
};

listener.Start();

// -------------------------------
// 🧠 Main accept loop
// -------------------------------
while (!cts.Token.IsCancellationRequested)
{
    try
    {
        var client = await listener.AcceptTcpClientAsync(cts.Token);
        _ = Task.Run(() => HandleClientAsync(client, port, cts.Token));
    }
    catch (OperationCanceledException)
    {
        break;
    }
    catch (Exception ex)
    {
        Console.WriteLine($"❌ Accept failed: {ex.Message}");
    }
}

listener.Stop();
Console.WriteLine("🛑 Backend stopped.");


// -------------------------------
// 🧩 Client handler
// -------------------------------
static async Task HandleClientAsync(TcpClient client, int port, CancellationToken token)
{
    Console.WriteLine($"🔗 Client connected → backend {port}");
    try
    {
        using var stream = client.GetStream();
        var buffer = new byte[4096];

        while (!token.IsCancellationRequested)
        {
            int read = await stream.ReadAsync(buffer.AsMemory(0, buffer.Length), token);
            if (read == 0) break; // disconnected

            var msg = Encoding.UTF8.GetString(buffer, 0, read);
            Console.WriteLine($"[{port}] Received: {msg}");

            var response = Encoding.UTF8.GetBytes($"[{port}] Echo: {msg}");
            await stream.WriteAsync(response, token);
        }
    }
    catch (OperationCanceledException)
    {
        // graceful shutdown
    }
    catch (Exception ex)
    {
        Console.WriteLine($"⚠️ Error in backend {port}: {ex.Message}");
    }
    finally
    {
        client.Close();
        Console.WriteLine($"❎ Client disconnected from backend {port}");
    }
}
