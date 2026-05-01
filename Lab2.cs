using System;
using System.Collections.Generic;
using System.Numerics;
using System.Xml.Linq;
using System.Threading.Tasks;

class Node
{
    public List<Node> Neighbors = new List<Node>();
    public int Included = -1; // local MIS size if included
    public int Excluded = -1; // local MIS size if excluded
}

class Program
{
    public static async Task FindMIS(Node v, Node parent = null)
    {
        int include = 1;
        int exclude = 0;

        var tasks = new List<Task>();

        foreach (var child in v.Neighbors)
        {
            if (child == parent) continue;

            tasks.Add(Task.Run(async () =>
            {
                await FindMIS(child, v);
            }));
        }

        await Task.WhenAll(tasks);

        foreach (var child in v.Neighbors)
        {
            if (child == parent) continue;

            include += child.Excluded;
            exclude += Math.Max(child.Included, child.Excluded);
        }

        v.Included = include;
        v.Excluded = exclude;
    }

    public static async Task RecoverMIS(Node node, Node parent, bool takeNode, HashSet<Node> result)
    {
        if (takeNode) // add node, skip children
        {
            result.Add(node);
            var tasks = new List<Task>();
            foreach (var child in node.Neighbors)
            {
                if (child == parent) continue;
                tasks.Add(RecoverMIS(child, node, false, result));
            }
            await Task.WhenAll(tasks);
        }
        else // skip node, check children
        {
            var tasks = new List<Task>();
            foreach (var child in node.Neighbors)
            {
                if (child == parent) continue;
                bool takeChild = child.Included > child.Excluded;
                tasks.Add(RecoverMIS(child, node, takeChild, result));
            }
            await Task.WhenAll(tasks);
        }
    }

    static async Task Main(string[] args)
    {
        // Example 1:
        //             0
        //           /   \
        //          1     2
        //         /    / | \
        //        3    4  5  6
        
        int n = 7;
        List<Node> nodes = new List<Node>(n);
        for (int i = 0; i < n; i++)
            nodes.Add(new Node());

        // Adding Edges to the Graph
        nodes[0].Neighbors.Add(nodes[1]); nodes[1].Neighbors.Add(nodes[0]);
        nodes[0].Neighbors.Add(nodes[2]); nodes[2].Neighbors.Add(nodes[0]);
        nodes[1].Neighbors.Add(nodes[3]); nodes[3].Neighbors.Add(nodes[1]);
        nodes[2].Neighbors.Add(nodes[4]); nodes[4].Neighbors.Add(nodes[2]);
        nodes[2].Neighbors.Add(nodes[5]); nodes[5].Neighbors.Add(nodes[2]);
        nodes[2].Neighbors.Add(nodes[6]); nodes[6].Neighbors.Add(nodes[2]);

        // Finding MIS via I[v] = max{1 + Σ(I[u]), Σ(I[w])},
        // where u - grandchildren of v, w - children of v

        // can use any node as root
        Node root = nodes[2];
        await FindMIS(root);

        // Restoring MIS
        HashSet<Node> misSet = new HashSet<Node>();
        bool takeRoot = root.Included > root.Excluded;
        await RecoverMIS(root, null, takeRoot, misSet);

        // getting Node indexes in MIS relative to List<Node>
        // (kinda overcomplicated but don't wanna use uid in Node class as it's not required)
        var indexes = misSet.Select(n => nodes.IndexOf(n)).OrderBy(i => i);
        
        Console.WriteLine("EXAMPLE 1");
        Console.WriteLine("MIS size: " + misSet.Count);
        Console.WriteLine("MIS nodes: " + string.Join(", ", indexes));

        // Example 2:
        //         0
        //      / / \ \
        //     1  2  3 4
        //    / \     / \
        //   5   6    7   8

        n = 9;
        nodes = new List<Node>(n);
        for (int i = 0; i < n; i++)
            nodes.Add(new Node());

        nodes[0].Neighbors.Add(nodes[1]); nodes[1].Neighbors.Add(nodes[0]);
        nodes[0].Neighbors.Add(nodes[2]); nodes[2].Neighbors.Add(nodes[0]);
        nodes[0].Neighbors.Add(nodes[3]); nodes[3].Neighbors.Add(nodes[0]);
        nodes[0].Neighbors.Add(nodes[4]); nodes[4].Neighbors.Add(nodes[0]);
        nodes[1].Neighbors.Add(nodes[5]); nodes[5].Neighbors.Add(nodes[1]);
        nodes[1].Neighbors.Add(nodes[6]); nodes[6].Neighbors.Add(nodes[1]);
        nodes[4].Neighbors.Add(nodes[7]); nodes[7].Neighbors.Add(nodes[4]);
        nodes[4].Neighbors.Add(nodes[8]); nodes[8].Neighbors.Add(nodes[4]);

        // can use any node as root
        root = nodes[4];
        await FindMIS(root);

        misSet = new HashSet<Node>();
        takeRoot = root.Included > root.Excluded;
        await RecoverMIS(root, null, takeRoot, misSet);

        indexes = misSet.Select(n => nodes.IndexOf(n)).OrderBy(i => i);
        
        Console.WriteLine("EXAMPLE 2");
        Console.WriteLine("MIS size: " + misSet.Count);
        Console.WriteLine("MIS nodes: " + string.Join(", ", indexes));
    }
}