using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Lab5_LocallyFinite
{
    // =====================================================================
    // Algorithm 1: Shortest Path (Bellman-Ford style, parallel per vertex)
    // =====================================================================
    class ShortestPathAlgorithm
    {
        private readonly int _n;
        private readonly int[,] _cost;  // cost[v,w] = edge weight, int.MaxValue/2 if no edge
        private readonly long[] _D;
        private readonly object _lock = new object();

        public ShortestPathAlgorithm(int n, List<(int u, int v, int w)> edges, int source)
        {
            _n = n;
            _cost = new int[n, n];
            _D = new long[n];

            // Init costs
            for (int i = 0; i < n; i++)
                for (int j = 0; j < n; j++)
                    _cost[i, j] = (i == j) ? 0 : int.MaxValue / 2;

            foreach (var (u, v, w) in edges)
            {
                _cost[u, v] = w;
                _cost[v, u] = w; // undirected
            }

            // Init D
            for (int i = 0; i < n; i++)
                _D[i] = (i == source) ? 0 : long.MaxValue / 2;
        }

        // minimization(v): D[v] = min(D[v], min over in-neighbors w: D[w] + c(w,v))
        private void Minimize(int v)
        {
            long best = _D[v];
            for (int w = 0; w < _n; w++)
            {
                if (_cost[w, v] < int.MaxValue / 2)
                {
                    long candidate = _D[w] + _cost[w, v];
                    if (candidate < best)
                        best = candidate;
                }
            }
            Interlocked.Exchange(ref _D[v], best);
        }

        public long[] Run()
        {
            // N iterations, each iteration: run minimization for all v in parallel
            for (int iter = 0; iter < _n; iter++)
            {
                var snapshot = (long[])_D.Clone(); // read consistent snapshot

                var threads = new Thread[_n];
                for (int v = 0; v < _n; v++)
                {
                    int vertex = v;
                    threads[v] = new Thread(() =>
                    {
                        long best = snapshot[vertex];
                        for (int w = 0; w < _n; w++)
                        {
                            if (_cost[w, vertex] < int.MaxValue / 2)
                            {
                                long candidate = snapshot[w] + _cost[w, vertex];
                                if (candidate < best)
                                    best = candidate;
                            }
                        }
                        Interlocked.Exchange(ref _D[vertex], best);
                    });
                }
                foreach (var t in threads) t.Start();
                foreach (var t in threads) t.Join();
            }
            return _D;
        }
    }

    // =====================================================================
    // Algorithm 2: Graph Coloring Minimization
    // =====================================================================
    class GraphColoringAlgorithm
    {
        private readonly int _n;
        private readonly List<List<int>> _adj;
        private readonly int[] _color;
        private readonly ReaderWriterLockSlim[] _locks;
        private int _iterations;
        private readonly Random _rng;

        public GraphColoringAlgorithm(int n, List<(int, int)> edges, int[] initialColors)
        {
            _n = n;
            _adj = Enumerable.Range(0, n).Select(_ => new List<int>()).ToList();
            _color = (int[])initialColors.Clone();
            _locks = Enumerable.Range(0, n).Select(_ => new ReaderWriterLockSlim()).ToArray();
            _rng = new Random(42);

            foreach (var (u, v) in edges)
            {
                _adj[u].Add(v);
                _adj[v].Add(u);
            }
        }

        // minimization(v): assign smallest color not used by any neighbor
        private void Minimize(int v)
        {
            // Read neighbor colors (using read locks)
            var neighborColors = new HashSet<int>();
            foreach (var u in _adj[v])
            {
                _locks[u].EnterReadLock();
                try { neighborColors.Add(_color[u]); }
                finally { _locks[u].ExitReadLock(); }
            }

            // Find smallest non-neighbor color
            int newColor = 0;
            while (neighborColors.Contains(newColor)) newColor++;

            _locks[v].EnterWriteLock();
            try { _color[v] = newColor; }
            finally { _locks[v].ExitWriteLock(); }

            Interlocked.Increment(ref _iterations);
        }

        public (int[] coloring, int iterations) Run()
        {
            // Determine convergence: run until no color changes in a full pass
            bool changed = true;
            while (changed)
            {
                var before = (int[])_color.Clone();

                // Choose random vertex and minimize (in parallel passes)
                // Run N random minimizations in parallel per round
                var threads = new Thread[_n];
                var order = Enumerable.Range(0, _n).OrderBy(_ => _rng.Next()).ToList();

                for (int i = 0; i < _n; i++)
                {
                    int v = order[i];
                    threads[i] = new Thread(() => Minimize(v));
                }
                foreach (var t in threads) t.Start();
                foreach (var t in threads) t.Join();

                var after = (int[])_color.Clone();
                changed = !before.SequenceEqual(after);
            }

            return (_color, _iterations);
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("=== Lab 5: Locally Finite Algorithms ===\n");

            // --- Algorithm 1: Shortest Path ---
            Console.WriteLine("--- Algorithm 1: Shortest Path (parallel Bellman-Ford) ---");
            int n = 6;
            var edges = new List<(int, int, int)>
            {
                (0, 1, 4), (0, 2, 1), (2, 1, 2), (1, 3, 1),
                (2, 3, 5), (3, 4, 3), (4, 5, 2), (1, 5, 10)
            };
            int source = 0;

            Console.WriteLine($"Graph: {n} vertices, source = {source}");
            Console.WriteLine("Edges: " + string.Join(", ", edges.Select(e => $"({e.Item1}-{e.Item2}, w={e.Item3})")));

            var spAlgo = new ShortestPathAlgorithm(n, edges, source);
            var distances = spAlgo.Run();

            Console.WriteLine($"\nShortest distances from vertex {source}:");
            for (int v = 0; v < n; v++)
                Console.WriteLine($"  D[{v}] = {(distances[v] >= long.MaxValue / 2 ? "∞" : distances[v].ToString())}");

            // --- Algorithm 2: Graph Coloring ---
            Console.WriteLine("\n--- Algorithm 2: Graph Coloring Minimization ---");
            int m = 7;
            var colorEdges = new List<(int, int)>
            {
                (0,1),(0,2),(1,2),(1,3),(2,4),(3,4),(3,5),(4,6),(5,6)
            };
            // Initial coloring (may be non-optimal but valid)
            int[] initialColors = { 0, 1, 2, 3, 4, 5, 6 };

            Console.WriteLine($"Graph: {m} vertices");
            Console.WriteLine("Initial coloring: [" + string.Join(", ", initialColors) + "]");

            var colorAlgo = new GraphColoringAlgorithm(m, colorEdges, initialColors);
            var (finalColoring, iters) = colorAlgo.Run();

            Console.WriteLine($"\nFinal coloring: [{string.Join(", ", finalColoring)}]");
            Console.WriteLine($"Colors used: {finalColoring.Distinct().Count()}");
            Console.WriteLine($"Minimization iterations: {iters}");

            // Verify: no two adjacent vertices have same color
            bool valid = colorEdges.All(e => finalColoring[e.Item1] != finalColoring[e.Item2]);
            Console.WriteLine($"Valid coloring: {valid}");

            Console.WriteLine("\nPress any key to exit...");
            Console.ReadKey();
        }
    }
}
