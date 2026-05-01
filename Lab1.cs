using System;
using System.Collections.Generic;

class Message
{
    public int OriginId;
    public int HopsLeft;
    public bool Direction; // false = left, true = right
    public bool IsReply;

    public Message(int originId, int hopsLeft, bool direction, bool isReply)
    {
        OriginId = originId;
        HopsLeft = hopsLeft;
        Direction = direction;
        this.IsReply = isReply;
    }
}

class Node
{
    public int Id;
    public Node Left, Right;
    public bool IsActive = true;
    public Queue<Message> Inbox = new();

    public Node(int id) { Id = id; }

    public void Send(Message msg, ref long totalMessages)
    {
        totalMessages++;
        if (msg.Direction) Right.Inbox.Enqueue(msg);
        else Left.Inbox.Enqueue(msg);
    }
}

class Program
{
    static void SendProbes(Node[] nodes, int radius, ref long totalMessages)
    {
        foreach (var node in nodes)
        {
            if (!node.IsActive) continue;
            node.Send(new Message(node.Id, radius, false, false), ref totalMessages);
            node.Send(new Message(node.Id, radius, true, false), ref totalMessages);
        }
    }

    static bool ProcessInbox(Node[] nodes, ref long totalMessages, ref Node leader)
    {
        bool progressed = false;

        foreach (var node in nodes)
        {
            int count = node.Inbox.Count;
            for (int i = 0; i < count; i++)
            {
                progressed = true;
                var msg = node.Inbox.Dequeue();
                HandleMessage(node, msg, ref totalMessages, ref leader);
            }
        }

        return progressed;
    }

    static void HandleMessage(Node node, Message msg, ref long totalMessages, ref Node leader)
    {
        if (!msg.IsReply)
        {
            if (msg.OriginId < node.Id)
            {
                // discard
            }
            else if (msg.OriginId > node.Id)
            {
                node.IsActive = false;
                if (msg.HopsLeft > 1)
                {
                    msg.HopsLeft--;
                    node.Send(msg, ref totalMessages);
                }
                else
                {
                    node.Send(new Message(msg.OriginId, 0, !msg.Direction, true), ref totalMessages);
                }
            }
            else
            {
                leader = node;
            }
        }
        else
        {
            if (msg.OriginId != node.Id)
            {
                node.Send(msg, ref totalMessages);
            }
        }
    }

    static void Main()
    {
        Console.Write("Input the number N: ");
        int n = int.Parse(Console.ReadLine());
        Node[] nodes = new Node[n];
        for (int i = 0; i < n; i++) nodes[i] = new Node(i + 1);
        for (int i = 0; i < n; i++)
        {
            nodes[i].Left = nodes[(i + n - 1) % n];
            nodes[i].Right = nodes[(i + 1) % n];
        }

        long totalMessages = 0;
        int rounds = 0;
        Node leader = null;

        while (leader == null)
        {
            rounds++;
            int radius = 1 << (rounds - 1);
            SendProbes(nodes, radius, ref totalMessages);

            while (ProcessInbox(nodes, ref totalMessages, ref leader)) { }
        }

        Console.WriteLine($"Leader: {leader.Id}");
        Console.WriteLine($"Rounds: {rounds}");
        Console.WriteLine($"Total messages: {totalMessages}");
    }
}