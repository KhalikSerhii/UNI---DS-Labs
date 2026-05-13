using System;
using System.Threading;
using System.Collections.Generic;

namespace Lab4_DiningPhilosophers
{
    // =====================================================================
    // Solution 1: One counting semaphore (limits max dining philosophers to N-1)
    // =====================================================================
    class DiningPhilosophers_OneSemaphore
    {
        private const int N = 5;
        private const int EAT_TIMES = 3;

        private readonly object[] _forks = new object[N];
        // Semaphore allows at most N-1 philosophers at table simultaneously
        // This prevents deadlock: at least one philosopher can always pick up both forks
        private readonly SemaphoreSlim _tableSemaphore = new SemaphoreSlim(N - 1, N - 1);

        public DiningPhilosophers_OneSemaphore()
        {
            for (int i = 0; i < N; i++) _forks[i] = new object();
        }

        public void Run()
        {
            Console.WriteLine("\n--- Solution 1: Single Counting Semaphore (N-1 = 4 max at table) ---");
            var threads = new Thread[N];
            for (int i = 0; i < N; i++)
            {
                int id = i;
                threads[i] = new Thread(() => PhilosopherRoutine(id));
                threads[i].Name = $"Philosopher-{id}";
            }
            foreach (var t in threads) t.Start();
            foreach (var t in threads) t.Join();
            Console.WriteLine("Solution 1 complete.\n");
        }

        private void PhilosopherRoutine(int id)
        {
            int left = id;
            int right = (id + 1) % N;

            for (int i = 0; i < EAT_TIMES; i++)
            {
                Think(id);

                // Acquire table semaphore (max N-1 can try at once)
                _tableSemaphore.Wait();

                lock (_forks[left])
                {
                    Log(id, "picked up LEFT fork");
                    lock (_forks[right])
                    {
                        Log(id, "picked up RIGHT fork -> EATING");
                        Thread.Sleep(new Random(id * 10 + i).Next(50, 150));
                        Log(id, "done eating, putting down forks");
                    }
                }

                _tableSemaphore.Release();
            }
            Log(id, "finished all meals.");
        }

        private void Think(int id)
        {
            Log(id, "thinking...");
            Thread.Sleep(new Random(id * 100).Next(30, 100));
        }

        private static void Log(int id, string msg)
        {
            Console.WriteLine($"[Philosopher {id}]: {msg}");
        }
    }

    // =====================================================================
    // Solution 2: Mutex + 5 individual fork semaphores
    // =====================================================================
    class DiningPhilosophers_MutexAndSemaphores
    {
        private const int N = 5;
        private const int EAT_TIMES = 3;

        // One semaphore per fork
        private readonly SemaphoreSlim[] _forkSems = new SemaphoreSlim[N];

        // Global mutex for shared state synchronization
        private readonly object _mutex = new object();

        // Philosopher states:
        // 0 = Thinking
        // 1 = Hungry
        // 2 = Eating
        private readonly int[] _state = new int[N];

        // Personal semaphores
        // Philosopher waits here until allowed to eat
        private readonly SemaphoreSlim[] _selfSems = new SemaphoreSlim[N];

        public DiningPhilosophers_MutexAndSemaphores()
        {
            for (int i = 0; i < N; i++)
            {
                _forkSems[i] = new SemaphoreSlim(1, 1);
                _selfSems[i] = new SemaphoreSlim(0, 1);
                _state[i] = 0;
            }
        }

        public void Run()
        {
            Console.WriteLine("=== Solution 2: Mutex + 5 Fork Semaphores ===\n");

            Thread[] threads = new Thread[N];

            for (int i = 0; i < N; i++)
            {
                int id = i;

                threads[i] = new Thread(() => PhilosopherRoutine(id));
                threads[i].Name = $"Philosopher-{id}";
            }

            foreach (Thread t in threads)
                t.Start();

            foreach (Thread t in threads)
                t.Join();

            Console.WriteLine("\nSolution 2 complete.");
        }

        private void PhilosopherRoutine(int id)
        {
            for (int i = 0; i < EAT_TIMES; i++)
            {
                Think(id);

                PickUpForks(id);

                Eat(id);

                PutDownForks(id);
            }

            Log(id, "FINISHED all meals");
        }

        private void PickUpForks(int id)
        {
            lock (_mutex)
            {
                _state[id] = 1;

                LogState(id, "became HUNGRY");
                PrintStates();

                TryToEat(id);
            }

            Log(id, "WAITING for permission to eat");

            _selfSems[id].Wait();

            Log(id, "RECEIVED permission to eat");
        }

        private void TryToEat(int id)
        {
            int leftNeighbor = (id - 1 + N) % N;
            int rightNeighbor = (id + 1) % N;

            int leftFork = leftNeighbor;
            int rightFork = id;

            Log(id,
                $"checking neighbors: LEFT={leftNeighbor}({_state[leftNeighbor]}), " +
                $"RIGHT={rightNeighbor}({_state[rightNeighbor]})");

            if (_state[id] == 1 &&
                _state[leftNeighbor] != 2 &&
                _state[rightNeighbor] != 2)
            {
                Log(id, "neighbors are NOT eating -> can eat");

                Log(id, $"trying to take LEFT fork {leftFork}");
                _forkSems[leftFork].Wait();
                Log(id, $"TOOK LEFT fork {leftFork}");

                Log(id, $"trying to take RIGHT fork {rightFork}");
                _forkSems[rightFork].Wait();
                Log(id, $"TOOK RIGHT fork {rightFork}");

                _state[id] = 2;

                LogState(id, "changed state to EATING");
                PrintStates();

                _selfSems[id].Release();

                Log(id, "signaled itself to START eating");
            }
            else
            {
                Log(id, "cannot eat now -> waiting");
            }
        }

        private void PutDownForks(int id)
        {
            int leftNeighbor = (id - 1 + N) % N;
            int rightNeighbor = (id + 1) % N;

            int leftFork = leftNeighbor;
            int rightFork = id;

            lock (_mutex)
            {
                _state[id] = 0;

                LogState(id, "changed state to THINKING");

                _forkSems[leftFork].Release();
                Log(id, $"RELEASED LEFT fork {leftFork}");

                _forkSems[rightFork].Release();
                Log(id, $"RELEASED RIGHT fork {rightFork}");

                PrintStates();

                Log(id, $"checking if LEFT neighbor {leftNeighbor} can eat");
                TryToEat(leftNeighbor);

                Log(id, $"checking if RIGHT neighbor {rightNeighbor} can eat");
                TryToEat(rightNeighbor);
            }
        }

        private void Think(int id)
        {
            LogState(id, "THINKING");

            Thread.Sleep(Random.Shared.Next(50, 150));
        }

        private void Eat(int id)
        {
            LogState(id, "EATING");

            Thread.Sleep(Random.Shared.Next(100, 250));

            Log(id, "FINISHED eating");
        }

        private void PrintStates()
        {
            string[] names = { "THINK", "HUNGRY", "EAT" };

            Console.Write("    STATES: ");

            for (int i = 0; i < N; i++)
            {
                Console.Write($"P{i}={names[_state[i]]} ");
            }

            Console.WriteLine();
        }

        private static void Log(int id, string msg)
        {
            Console.WriteLine($"[Philosopher {id}] {msg}");
        }

        private static void LogState(int id, string state)
        {
            Console.WriteLine($"[Philosopher {id}] STATE -> {state}");
        }
    }


    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("=== Lab 4: Dining Philosophers Problem ===");
            Console.WriteLine("N = 5 philosophers, each eats 3 times\n");

            var sol1 = new DiningPhilosophers_OneSemaphore();
            sol1.Run();

            var sol2 = new DiningPhilosophers_MutexAndSemaphores();
            sol2.Run();

            Console.WriteLine("All solutions finished. Press any key to exit...");
            Console.ReadKey();
        }
    }
}
