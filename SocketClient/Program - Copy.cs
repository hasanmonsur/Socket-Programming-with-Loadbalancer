using System.Net.Sockets;
using System.Text;

Console.WriteLine("Welcome to Socket Client");


var client = new TcpClient();
await client.ConnectAsync("192.168.1.7", 9000); // Connect to Gateway

using var stream = client.GetStream();
Console.WriteLine("Connected to gateway");

while (true)
{
    Console.Write("Send: ");
    var msg = Console.ReadLine();
    if (msg == "exit") break;

    if (msg is null)
        continue;

    var data = Encoding.UTF8.GetBytes(msg);
    await stream.WriteAsync(data);

    var buffer = new byte[4096];
    int read = await stream.ReadAsync(buffer);
    Console.WriteLine("Received: " + Encoding.UTF8.GetString(buffer, 0, read));
}
