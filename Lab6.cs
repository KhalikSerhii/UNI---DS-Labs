using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Pipes;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
namespace Lab6_AtomicSwap
{
    // =====================================================================
    // HTLC - Hash Time Lock Contract simulation
    // Three-party atomic swap: Alice -> Bob -> Charlie
    // Alice sends 1 CoinA to Bob
    // Bob  sends 1 CoinB to Charlie
    // Charlie sends 1 CoinC to Alice
    //
    // Each party runs as a SEPARATE OS PROCESS, communicating via
    // named pipes. The Coordinator process spawns Alice, Bob, Charlie,
    // distributes parameters, and observes the result.
    //
    // Architecture:
    //   Coordinator  →  spawns  →  Alice (role=alice)
    //                             Bob   (role=bob)
    //                             Charlie (role=charlie)
    //
    //   Pipes (named, duplex simulation via two half-duplex pipes):
    //     coordinator-alice-in / coordinator-alice-out
    //     coordinator-bob-in   / coordinator-bob-out
    //     coordinator-charlie-in / coordinator-charlie-out
    //
    // Message protocol (JSON lines):
    //   { "type": "INIT",    "hashLock": "...", "timeLock": 30 }
    //   { "type": "SECRET",  "value": "..." }
    //   { "type": "REDEEM",  "contractId": "CA", "secret": "..." }
    //   { "type": "REFUND",  "contractId": "AB2" }
    //   { "type": "STATUS",  "contractId": "...", "state": "redeemed|refunded|pending" }
    //   { "type": "LOG",     "party": "...", "msg": "...", "color": 0..15 }
    //   { "type": "DONE" }
    // =====================================================================

    // ── Shared helpers ──────────────────────────────────────────────────

    static class Logger
    {
        private static readonly object _lock = new();

        public static void Log(string party, string msg,
            ConsoleColor color = ConsoleColor.White)
        {
            lock (_lock)
            {
                Console.ForegroundColor = color;
                Console.WriteLine(
                    $"[{DateTime.Now:HH:mm:ss.fff}] [{party,-10}] {msg}");
                Console.ResetColor();
            }
        }
    }

    // ── HTLC contract (shared data model, instantiated in each process) ─

    class HTLCContract
    {
        public string Id         { get; }
        public string Sender     { get; }
        public string Receiver   { get; }
        public decimal Amount    { get; }
        public string CoinType   { get; }
        public string HashLock   { get; }
        public DateTime ExpiresAt { get; }

        public bool IsRedeemed  { get; private set; }
        public bool IsRefunded  { get; private set; }
        public string? RevealedSecret { get; private set; }

        private readonly object _lock = new();

        public HTLCContract(string id, string sender, string receiver,
            decimal amount, string coinType, string hashLock,
            int timeLockSeconds)
        {
            Id        = id;
            Sender    = sender;
            Receiver  = receiver;
            Amount    = amount;
            CoinType  = coinType;
            HashLock  = hashLock;
            ExpiresAt = DateTime.UtcNow.AddSeconds(timeLockSeconds);
        }

        public bool Redeem(string secret)
        {
            lock (_lock)
            {
                if (IsRedeemed || IsRefunded)
                {
                    Logger.Log("CONTRACT",
                        $"[{Id}] Cannot redeem: already settled.",
                        ConsoleColor.Red);
                    return false;
                }
                if (DateTime.UtcNow > ExpiresAt)
                {
                    Logger.Log("CONTRACT",
                        $"[{Id}] Cannot redeem: timelock expired.",
                        ConsoleColor.Red);
                    return false;
                }
                if (ComputeHash(secret) != HashLock)
                {
                    Logger.Log("CONTRACT",
                        $"[{Id}] Invalid secret.", ConsoleColor.Red);
                    return false;
                }

                IsRedeemed     = true;
                RevealedSecret = secret;
                Logger.Log("CONTRACT",
                    $"[{Id}] REDEEMED by {Receiver}: " +
                    $"{Amount} {CoinType} transferred. Secret='{secret}'",
                    ConsoleColor.Green);
                return true;
            }
        }

        public bool Refund()
        {
            lock (_lock)
            {
                if (IsRedeemed || IsRefunded)
                {
                    Logger.Log("CONTRACT",
                        $"[{Id}] Cannot refund: already settled.",
                        ConsoleColor.Red);
                    return false;
                }
                if (DateTime.UtcNow <= ExpiresAt)
                {
                    var rem = (ExpiresAt - DateTime.UtcNow).TotalSeconds;
                    Logger.Log("CONTRACT",
                        $"[{Id}] Cannot refund: " +
                        $"timelock not expired ({rem:F1}s remaining).",
                        ConsoleColor.Yellow);
                    return false;
                }

                IsRefunded = true;
                Logger.Log("CONTRACT",
                    $"[{Id}] REFUNDED to {Sender}: " +
                    $"{Amount} {CoinType} returned.",
                    ConsoleColor.Yellow);
                return true;
            }
        }

        public static string ComputeHash(string secret)
        {
            using var sha   = SHA256.Create();
            var bytes       = sha.ComputeHash(Encoding.UTF8.GetBytes(secret));
            return Convert.ToHexString(bytes);
        }

        public override string ToString() =>
            $"HTLC[{Id}]: {Sender}->{Receiver}, " +
            $"{Amount} {CoinType}, expires={ExpiresAt:HH:mm:ss}";
    }

    // ── Pipe-based message channel ──────────────────────────────────────

    // Used by the Coordinator to talk to one party process.
    class CoordinatorChannel : IDisposable
    {
        private readonly NamedPipeServerStream _toParty;   // coord → party
        private readonly NamedPipeServerStream _fromParty; // party → coord
        private StreamWriter  _writer = null!;
        private StreamReader  _reader = null!;

        public string Role { get; }

        public CoordinatorChannel(string role)
        {
            Role = role;
            // Two half-duplex pipes simulate full-duplex
            _toParty   = new NamedPipeServerStream(
                $"htlc-{role}-in",  PipeDirection.Out,
                1, PipeTransmissionMode.Byte,
                PipeOptions.Asynchronous);
            _fromParty = new NamedPipeServerStream(
                $"htlc-{role}-out", PipeDirection.In,
                1, PipeTransmissionMode.Byte,
                PipeOptions.Asynchronous);
        }

        public void WaitForConnections()
        {
            _toParty.WaitForConnection();
            _fromParty.WaitForConnection();
            _writer = new StreamWriter(_toParty)   { AutoFlush = true };
            _reader = new StreamReader(_fromParty);
        }

        public void Send(JsonObject msg) =>
            _writer.WriteLine(msg.ToJsonString());

        public string? ReadLine() => _reader.ReadLine();

        public void Dispose()
        {
            _writer?.Dispose();
            _reader?.Dispose();
            _toParty.Dispose();
            _fromParty.Dispose();
        }
    }

    // Used by a party process to talk back to the Coordinator.
    class PartyChannel : IDisposable
    {
        private readonly NamedPipeClientStream _fromCoord; // coord → party
        private readonly NamedPipeClientStream _toCoord;   // party → coord
        private StreamReader _reader = null!;
        private StreamWriter _writer = null!;

        public PartyChannel(string role)
        {
            _fromCoord = new NamedPipeClientStream(
                ".", $"htlc-{role}-in",  PipeDirection.In);
            _toCoord   = new NamedPipeClientStream(
                ".", $"htlc-{role}-out", PipeDirection.Out);
        }

        public void Connect()
        {
            _fromCoord.Connect(5000);
            _toCoord.Connect(5000);
            _reader = new StreamReader(_fromCoord);
            _writer = new StreamWriter(_toCoord) { AutoFlush = true };
        }

        public string? ReadLine() => _reader.ReadLine();

        public void Send(JsonObject msg) =>
            _writer.WriteLine(msg.ToJsonString());

        public void Dispose()
        {
            _reader?.Dispose();
            _writer?.Dispose();
            _fromCoord.Dispose();
            _toCoord.Dispose();
        }
    }

    // ── Party process logic ─────────────────────────────────────────────

    // Each party runs in its own OS process (spawned by the Coordinator).
    // It connects to the coordinator via named pipes and reacts to messages.

    static class AliceProcess
    {
        // Alice: generates the secret, initiates redemption of CoinC,
        //        then learns whether the full swap succeeded.
        public static void Run()
        {
            using var ch = new PartyChannel("alice");
            ch.Connect();
            Logger.Log("Alice", "Process started, connected to coordinator.",
                ConsoleColor.Cyan);

            while (true)
            {
                var line = ch.ReadLine();
                if (line == null) break;

                var doc  = JsonDocument.Parse(line);
                var type = doc.RootElement.GetProperty("type").GetString();

                if (type == "INIT")
                {
                    var hashLock   = doc.RootElement
                        .GetProperty("hashLock").GetString()!;
                    var timeLockCA = doc.RootElement
                        .GetProperty("timeLockCA").GetInt32();
                    Logger.Log("Alice",
                        $"Received INIT. HashLock={hashLock[..16]}... " +
                        $"TimeLockCA={timeLockCA}s", ConsoleColor.Cyan);

                    // Tell coord: ready
                    ch.Send(new JsonObject { ["type"] = "READY" });
                }
                else if (type == "REDEEM")
                {
                    var contractId = doc.RootElement
                        .GetProperty("contractId").GetString()!;
                    var secret     = doc.RootElement
                        .GetProperty("secret").GetString()!;

                    Logger.Log("Alice",
                        $"Redeeming contract {contractId} " +
                        $"with secret '{secret}'", ConsoleColor.Cyan);

                    // Acknowledge; coordinator owns the contract objects
                    ch.Send(new JsonObject {
                        ["type"] = "REDEEMED",
                        ["contractId"] = contractId,
                        ["secret"] = secret
                    });
                }
                else if (type == "STATUS")
                {
                    var state = doc.RootElement
                        .GetProperty("state").GetString();
                    Logger.Log("Alice",
                        $"Swap final status: {state}", ConsoleColor.Cyan);
                    ch.Send(new JsonObject { ["type"] = "ACK" });
                }
                else if (type == "DONE")
                {
                    Logger.Log("Alice", "Shutting down.", ConsoleColor.Cyan);
                    break;
                }
            }
        }
    }

    static class BobProcess
    {
        public static void Run()
        {
            using var ch = new PartyChannel("bob");
            ch.Connect();
            Logger.Log("Bob", "Process started, connected to coordinator.",
                ConsoleColor.Blue);

            while (true)
            {
                var line = ch.ReadLine();
                if (line == null) break;

                var doc  = JsonDocument.Parse(line);
                var type = doc.RootElement.GetProperty("type").GetString();

                if (type == "INIT")
                {
                    var hashLock   = doc.RootElement
                        .GetProperty("hashLock").GetString()!;
                    var timeLockBC = doc.RootElement
                        .GetProperty("timeLockBC").GetInt32();
                    Logger.Log("Bob",
                        $"Received INIT. HashLock={hashLock[..16]}... " +
                        $"TimeLockBC={timeLockBC}s", ConsoleColor.Blue);
                    ch.Send(new JsonObject { ["type"] = "READY" });
                }
                else if (type == "SECRET")
                {
                    // Charlie revealed the secret on-chain; Bob now redeems CoinA
                    var secret = doc.RootElement
                        .GetProperty("value").GetString()!;
                    Logger.Log("Bob",
                        $"Observed revealed secret '{secret}' on chain. " +
                        $"Will redeem CoinA.", ConsoleColor.Blue);
                    ch.Send(new JsonObject {
                        ["type"] = "REDEEMED",
                        ["contractId"] = "AB",
                        ["secret"] = secret
                    });
                }
                else if (type == "REFUND")
                {
                    var contractId = doc.RootElement
                        .GetProperty("contractId").GetString()!;
                    Logger.Log("Bob",
                        $"Requesting refund on contract {contractId}.",
                        ConsoleColor.Blue);
                    ch.Send(new JsonObject {
                        ["type"] = "REFUNDED",
                        ["contractId"] = contractId
                    });
                }
                else if (type == "STATUS")
                {
                    var state = doc.RootElement
                        .GetProperty("state").GetString();
                    Logger.Log("Bob",
                        $"Swap final status: {state}", ConsoleColor.Blue);
                    ch.Send(new JsonObject { ["type"] = "ACK" });
                }
                else if (type == "DONE")
                {
                    Logger.Log("Bob", "Shutting down.", ConsoleColor.Blue);
                    break;
                }
            }
        }
    }

    static class CharlieProcess
    {
        public static void Run()
        {
            using var ch = new PartyChannel("charlie");
            ch.Connect();
            Logger.Log("Charlie",
                "Process started, connected to coordinator.",
                ConsoleColor.Magenta);

            while (true)
            {
                var line = ch.ReadLine();
                if (line == null) break;

                var doc  = JsonDocument.Parse(line);
                var type = doc.RootElement.GetProperty("type").GetString();

                if (type == "INIT")
                {
                    var hashLock   = doc.RootElement
                        .GetProperty("hashLock").GetString()!;
                    var timeLockCA = doc.RootElement
                        .GetProperty("timeLockCA").GetInt32();
                    Logger.Log("Charlie",
                        $"Received INIT. HashLock={hashLock[..16]}... " +
                        $"TimeLockCA={timeLockCA}s", ConsoleColor.Magenta);
                    ch.Send(new JsonObject { ["type"] = "READY" });
                }
                else if (type == "SECRET")
                {
                    // Alice revealed the secret when she redeemed CA;
                    // Charlie now redeems CoinB from Bob's BC contract.
                    var secret = doc.RootElement
                        .GetProperty("value").GetString()!;
                    Logger.Log("Charlie",
                        $"Observed revealed secret '{secret}' on chain. " +
                        $"Will redeem CoinB.", ConsoleColor.Magenta);
                    ch.Send(new JsonObject {
                        ["type"] = "REDEEMED",
                        ["contractId"] = "BC",
                        ["secret"] = secret
                    });
                }
                else if (type == "STATUS")
                {
                    var state = doc.RootElement
                        .GetProperty("state").GetString();
                    Logger.Log("Charlie",
                        $"Swap final status: {state}", ConsoleColor.Magenta);
                    ch.Send(new JsonObject { ["type"] = "ACK" });
                }
                else if (type == "DONE")
                {
                    Logger.Log("Charlie", "Shutting down.", ConsoleColor.Magenta);
                    break;
                }
            }
        }
    }

    // ── Ledger (coordinator-side only) ──────────────────────────────────

    class Ledger
    {
        private readonly Dictionary<(string, string), decimal> _bal = new();
        private readonly object _lock = new();

        public void Set(string party, string coin, decimal amount)
        {
            lock (_lock) _bal[(party, coin)] = amount;
        }

        public decimal Get(string party, string coin)
        {
            lock (_lock)
            {
                _bal.TryGetValue((party, coin), out var v);
                return v;
            }
        }

        public void PrintAll()
        {
            lock (_lock)
            {
                Console.WriteLine("\n  Balances:");
                foreach (var ((p, c), a) in _bal)
                    Console.WriteLine($"    {p,-10} {c,-6}: {a:F2}");
            }
        }
    }

    // ── Coordinator ─────────────────────────────────────────────────────

    // Runs in the main process, manages all three HTLC contracts,
    // orchestrates messages, and drives both scenarios.

    class Coordinator
    {
        private readonly Ledger _ledger = new();

        // ── helper: spawn a party sub-process ──────────────────────────
        private static Process SpawnParty(string role)
        {
            var exe  = Process.GetCurrentProcess().MainModule!.FileName;
            var psi  = new ProcessStartInfo(exe, $"--role {role}")
            {
                UseShellExecute  = false,
                RedirectStandardInput  = false,
                RedirectStandardOutput = false,
                RedirectStandardError  = false,
            };
            var p = Process.Start(psi)
                ?? throw new Exception($"Failed to spawn {role}");
            Logger.Log("COORD",
                $"Spawned {role} as PID {p.Id}.", ConsoleColor.White);
            return p;
        }

        // ── Scenario 1: successful swap ─────────────────────────────────
        public void RunSuccessScenario()
        {
            Console.WriteLine(
                "\n========== SCENARIO 1: Successful Atomic Swap ==========");

            _ledger.Set("Alice",   "CoinA", 10);
            _ledger.Set("Bob",     "CoinB", 10);
            _ledger.Set("Charlie", "CoinC", 10);

            Console.WriteLine("\nInitial state:");
            _ledger.PrintAll();

            // Alice generates the secret (only she knows it initially)
            string secret   = "SuperSecretPreimage_XYZ_2024";
            string hashLock = HTLCContract.ComputeHash(secret);
            Logger.Log("COORD",
                $"Alice's secret (private): '{secret}'", ConsoleColor.White);
            Logger.Log("COORD",
                $"Public hashLock: {hashLock[..16]}...", ConsoleColor.White);

            // Create channels (pipes)
            using var chAlice   = new CoordinatorChannel("alice");
            using var chBob     = new CoordinatorChannel("bob");
            using var chCharlie = new CoordinatorChannel("charlie");

            // Spawn party processes
            var pAlice   = SpawnParty("alice");
            var pBob     = SpawnParty("bob");
            var pCharlie = SpawnParty("charlie");

            // Wait for all pipes to connect (parties connect in ~100 ms)
            var t1 = new Thread(() => chAlice.WaitForConnections());
            var t2 = new Thread(() => chBob.WaitForConnections());
            var t3 = new Thread(() => chCharlie.WaitForConnections());
            t1.Start(); t2.Start(); t3.Start();
            t1.Join();  t2.Join();  t3.Join();

            Logger.Log("COORD", "All parties connected.", ConsoleColor.White);

            // ── Step 1: create HTLC contracts ───────────────────────────
            // Alice locks 1 CoinA → Bob   (30 s)
            var contractAB = new HTLCContract(
                "AB", "Alice", "Bob", 1, "CoinA", hashLock, 30);
            // Bob locks   1 CoinB → Charlie (20 s)
            var contractBC = new HTLCContract(
                "BC", "Bob", "Charlie", 1, "CoinB", hashLock, 20);
            // Charlie locks 1 CoinC → Alice (10 s)
            var contractCA = new HTLCContract(
                "CA", "Charlie", "Alice", 1, "CoinC", hashLock, 10);

            Logger.Log("COORD", $"Contracts created: {contractAB}", ConsoleColor.White);
            Logger.Log("COORD", $"                   {contractBC}", ConsoleColor.White);
            Logger.Log("COORD", $"                   {contractCA}", ConsoleColor.White);

            // ── Step 2: INIT each party ─────────────────────────────────
            chAlice.Send(new JsonObject {
                ["type"] = "INIT",
                ["hashLock"] = hashLock,
                ["timeLockCA"] = 10
            });
            chBob.Send(new JsonObject {
                ["type"] = "INIT",
                ["hashLock"] = hashLock,
                ["timeLockBC"] = 20
            });
            chCharlie.Send(new JsonObject {
                ["type"] = "INIT",
                ["hashLock"] = hashLock,
                ["timeLockCA"] = 10
            });

            // Await READY from each
            ExpectType(chAlice.ReadLine(),   "READY", "Alice");
            ExpectType(chBob.ReadLine(),     "READY", "Bob");
            ExpectType(chCharlie.ReadLine(), "READY", "Charlie");

            Thread.Sleep(300); // simulate network propagation

            // ── Step 3: Alice redeems CoinC (reveals secret) ────────────
            chAlice.Send(new JsonObject {
                ["type"] = "REDEEM",
                ["contractId"] = "CA",
                ["secret"] = secret
            });
            var aliceMsg = JsonDocument.Parse(chAlice.ReadLine()!);
            contractCA.Redeem(secret); // coordinator executes the contract

            // ── Step 4: Charlie observes secret on-chain, redeems CoinB ─
            string revealedSecret = contractCA.RevealedSecret!;
            chCharlie.Send(new JsonObject {
                ["type"] = "SECRET",
                ["value"] = revealedSecret
            });
            var charlieMsg = JsonDocument.Parse(chCharlie.ReadLine()!);
            contractBC.Redeem(revealedSecret);

            // ── Step 5: Bob observes secret, redeems CoinA ──────────────
            chBob.Send(new JsonObject {
                ["type"] = "SECRET",
                ["value"] = revealedSecret
            });
            var bobMsg = JsonDocument.Parse(chBob.ReadLine()!);
            contractAB.Redeem(revealedSecret);

            // ── Step 6: notify all parties of final status ──────────────
            string state = (contractAB.IsRedeemed &&
                            contractBC.IsRedeemed &&
                            contractCA.IsRedeemed)
                ? "SWAP_SUCCEEDED" : "SWAP_FAILED";

            foreach (var ch in new[] { chAlice, chBob, chCharlie })
            {
                ch.Send(new JsonObject { ["type"] = "STATUS", ["state"] = state });
                ch.ReadLine(); // ACK
                ch.Send(new JsonObject { ["type"] = "DONE" });
            }

            // Update balances
            if (state == "SWAP_SUCCEEDED")
            {
                _ledger.Set("Alice",   "CoinA", _ledger.Get("Alice",   "CoinA") - 1);
                _ledger.Set("Alice",   "CoinC", _ledger.Get("Alice",   "CoinC") + 1);
                _ledger.Set("Bob",     "CoinB", _ledger.Get("Bob",     "CoinB") - 1);
                _ledger.Set("Bob",     "CoinA", _ledger.Get("Bob",     "CoinA") + 1);
                _ledger.Set("Charlie", "CoinC", _ledger.Get("Charlie", "CoinC") - 1);
                _ledger.Set("Charlie", "CoinB", _ledger.Get("Charlie", "CoinB") + 1);

                Console.WriteLine("\nFinal state (SWAP SUCCEEDED):");
                _ledger.PrintAll();
            }

            pAlice.WaitForExit(3000);
            pBob.WaitForExit(3000);
            pCharlie.WaitForExit(3000);
        }

        // ── Scenario 2: timeout / refund ───────────────────────────────
        public void RunRefundScenario()
        {
            Console.WriteLine(
                "\n========== SCENARIO 2: Failed Swap – Refund After Timeout ==========");

            string secret   = "AnotherSecret_ABC";
            string hashLock = HTLCContract.ComputeHash(secret);

            using var chAlice = new CoordinatorChannel("alice");
            using var chBob   = new CoordinatorChannel("bob");

            var pAlice = SpawnParty("alice");
            var pBob   = SpawnParty("bob");

            var t1 = new Thread(() => chAlice.WaitForConnections());
            var t2 = new Thread(() => chBob.WaitForConnections());
            t1.Start(); t2.Start();
            t1.Join();  t2.Join();

            // Very short timelock (1 s) so we can observe expiry quickly
            var contractAB = new HTLCContract(
                "AB2", "Alice", "Bob", 1, "CoinA", hashLock, 1);
            Logger.Log("COORD", $"Contract: {contractAB}", ConsoleColor.White);

            // Init Alice & Bob
            chAlice.Send(new JsonObject {
                ["type"] = "INIT", ["hashLock"] = hashLock, ["timeLockCA"] = 1
            });
            chBob.Send(new JsonObject {
                ["type"] = "INIT", ["hashLock"] = hashLock, ["timeLockBC"] = 1
            });
            ExpectType(chAlice.ReadLine(), "READY", "Alice");
            ExpectType(chBob.ReadLine(),   "READY", "Bob");

            Logger.Log("COORD",
                "Bob is uncooperative — not redeeming.", ConsoleColor.Gray);
            Logger.Log("COORD",
                "Waiting for timelock to expire (1.5 s)…", ConsoleColor.Gray);
            Thread.Sleep(1500);

            // Alice requests refund
            Logger.Log("COORD",
                "Requesting refund for Alice on AB2…", ConsoleColor.White);
            contractAB.Refund();

            // Notify Bob of the refund outcome (he gets nothing)
            chBob.Send(new JsonObject {
                ["type"] = "REFUND", ["contractId"] = "AB2"
            });
            chBob.ReadLine(); // REFUNDED ack

            string state = "SWAP_FAILED_REFUNDED";
            chAlice.Send(new JsonObject { ["type"] = "STATUS", ["state"] = state });
            chAlice.ReadLine(); // ACK
            chBob.Send(new JsonObject   { ["type"] = "STATUS", ["state"] = state });
            chBob.ReadLine();   // ACK

            chAlice.Send(new JsonObject { ["type"] = "DONE" });
            chBob.Send(new JsonObject   { ["type"] = "DONE" });

            pAlice.WaitForExit(3000);
            pBob.WaitForExit(3000);

            Console.WriteLine("\nRefund scenario complete. Alice keeps CoinA.");
        }

        private static void ExpectType(string? line, string expected, string who)
        {
            if (line == null)
                throw new Exception($"Pipe closed waiting for {expected} from {who}");
            var doc  = JsonDocument.Parse(line);
            var type = doc.RootElement.GetProperty("type").GetString();
            if (type != expected)
                throw new Exception(
                    $"Expected {expected} from {who}, got {type}");
            Logger.Log("COORD",
                $"Received {type} from {who}.", ConsoleColor.DarkGray);
        }
    }

    // ── Entry point ─────────────────────────────────────────────────────

    class Program
    {
        static void Main(string[] args)
        {
            // Sub-process mode: --role alice | bob | charlie
            if (args.Length >= 2 && args[0] == "--role")
            {
                switch (args[1].ToLower())
                {
                    case "alice":   AliceProcess.Run();   return;
                    case "bob":     BobProcess.Run();     return;
                    case "charlie": CharlieProcess.Run(); return;
                    default:
                        Console.Error.WriteLine($"Unknown role: {args[1]}");
                        return;
                }
            }

            // Coordinator (main process)
            Console.WriteLine("=== Lab 6: Three-Party Atomic Swap using HTLC ===");
            Console.WriteLine("Coordinator PID: " +
                Process.GetCurrentProcess().Id);
            Console.WriteLine(
                "Parties: Alice, Bob, Charlie (each in a separate OS process)");
            Console.WriteLine("Coins:   CoinA, CoinB, CoinC\n");

            var coord = new Coordinator();
            coord.RunSuccessScenario();
            coord.RunRefundScenario();

            Console.WriteLine("\n=== Done. Press any key to exit… ===");
            Console.ReadKey();
        }
    }
}