using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace Lab3
{
    enum MsgType { Priority, JoinMIS, Eliminate, Done }

    record Msg(MsgType Type, int From, int Round, double Priority = 0);

    class Vertex
    {
        public int Id { get; }
        public bool InMIS { get; private set; }

        private readonly Channel<Msg> inbox;
        private readonly List<Vertex> neighbors = new();
        private readonly Random rng;

        public Vertex(int id, int seed)
        {
            Id = id;
            rng = new Random(seed);
            inbox = Channel.CreateUnbounded<Msg>(
                new UnboundedChannelOptions { SingleReader = true });
        }

        public ChannelWriter<Msg> Inbox => inbox.Writer;

        public void AddNeighbor(Vertex v) => neighbors.Add(v);

        private void Send(Vertex v, Msg m)
        {
            v.Inbox.TryWrite(m);
        }

        public async Task RunAsync(CancellationToken ct)
        {
            if (neighbors.Count == 0)
            {
                InMIS = true;
                return;
            }

            bool active = true;
            int round = 0;

            while (active && !ct.IsCancellationRequested)
            {
                round++;
                double myPriority = rng.NextDouble();

                // ── broadcast priority ─────────────────────────────
                foreach (var n in neighbors)
                    Send(n, new Msg(MsgType.Priority, Id, round, myPriority));

                // ── collect neighbor priorities ───────────────────
                var seen = new Dictionary<int, double>();

                while (seen.Count < neighbors.Count)
                {
                    var msg = await inbox.Reader.ReadAsync(ct);
                    if (msg.Round != round || msg.Type != MsgType.Priority)
                        continue;

                    seen[msg.From] = msg.Priority;
                }

                bool isMax = seen.Values.All(p => myPriority > p);

                if (isMax)
                {
                    InMIS = true;
                    active = false;

                    foreach (var n in neighbors)
                        Send(n, new Msg(MsgType.JoinMIS, Id, round));
                }

                // ── barrier sync ─────────────────────────────────
                foreach (var n in neighbors)
                    Send(n, new Msg(MsgType.Done, Id, round));

                int done = 0;
                bool eliminated = false;

                while (done < neighbors.Count)
                {
                    var msg = await inbox.Reader.ReadAsync(ct);
                    if (msg.Round != round) continue;

                    switch (msg.Type)
                    {
                        case MsgType.Done:
                            done++;
                            break;

                        case MsgType.JoinMIS:
                            if (!isMax)
                                eliminated = true;
                            break;
                    }
                }

                if (eliminated)
                    active = false;
            }
        }
    }

    class Graph
    {
        public List<Vertex> V;

        public Graph(int n, List<(int u, int v)> edges)
        {
            V = Enumerable.Range(0, n)
                .Select(i => new Vertex(i, i * 97 + 11))
                .ToList();

            foreach (var (u, v) in edges)
            {
                V[u].AddNeighbor(V[v]);
                V[v].AddNeighbor(V[u]);
            }
        }

        public async Task<List<int>> RunAsync()
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

            await Task.WhenAll(V.Select(v => v.RunAsync(cts.Token)));

            return V.Where(v => v.InMIS)
                    .Select(v => v.Id)
                    .OrderBy(x => x)
                    .ToList();
        }

        public bool Validate(List<int> mis, List<(int u, int v)> edges)
        {
            var set = new HashSet<int>(mis);

            foreach (var (u, v) in edges)
                if (set.Contains(u) && set.Contains(v))
                    return false;

            return true;
        }
    }

    class Program
    {
        static async Task RunTest(string name, int n, List<(int, int)> edges)
        {
            Console.WriteLine($"--- {name} ---");

            var g = new Graph(n, edges);
            var mis = await g.RunAsync();

            if(g.Validate(mis, edges))
                Console.WriteLine($"MIS: [{string.Join(", ", mis)}], size={mis.Count}");
            else
                Console.WriteLine("Result is not a MIS");
        }

        static async Task Main()
        {
            Console.WriteLine("=== Luby MIS ===\n");

            await RunTest("Cycle C6",
                6, new() { (0,1),(1,2),(2,3),(3,4),(4,5),(5,0) });

            await RunTest("Complete K5",
                5, new() {
                    (0,1),(0,2),(0,3),(0,4),
                    (1,2),(1,3),(1,4),
                    (2,3),(2,4),
                    (3,4)
                });

            await RunTest("Path P7",
                7, new() { (0,1),(1,2),(2,3),(3,4),(4,5),(5,6) });
        }
    }
}