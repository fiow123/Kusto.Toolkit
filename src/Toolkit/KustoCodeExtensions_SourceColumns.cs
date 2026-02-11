using System;
using System.Collections.Generic;
using System.Linq;
using System.Xml.Linq;
using Kusto.Language;
using Kusto.Language.Symbols;
using Kusto.Language.Syntax;
using Kusto.Language.Utils;

#nullable disable // some day...

namespace Kusto.Toolkit
{
    /// <summary>
    /// Represents a source column with optional function call chain attribution.
    /// </summary>
    public class SourceColumnInfo
    {
        /// <summary>
        /// The terminal database table column that is the ultimate source of the data.
        /// </summary>
        public ColumnSymbol Column { get; }

        /// <summary>
        /// The ordered list of function calls traversed to reach this source column,
        /// from outermost (closest to the query) to innermost (closest to the table).
        /// Empty if the column was accessed directly without going through any function.
        /// </summary>
        public IReadOnlyList<FunctionSymbol> FunctionCallChain { get; }

        public SourceColumnInfo(ColumnSymbol column, IReadOnlyList<FunctionSymbol> functionCallChain)
        {
            Column = column ?? throw new ArgumentNullException(nameof(column));
            FunctionCallChain = functionCallChain ?? Array.Empty<FunctionSymbol>();
        }

        public SourceColumnInfo(ColumnSymbol column)
            : this(column, Array.Empty<FunctionSymbol>())
        {
        }
    }

    public static partial class KustoCodeExtensions
    {
        /// <summary>
        /// Returns the set of database table columns that contributed to the data contained in the result columns.
        /// </summary>
        public static IReadOnlyList<ColumnSymbol> GetSourceColumns(this KustoCode code)
        {
            return GetSourceColumns(code, GetResultColumns(code));
        }

        /// <summary>
        /// Returns the set of database table columns that contributed to the data contained in the specified column.
        /// </summary>
        public static IReadOnlyList<ColumnSymbol> GetSourceColumns(this KustoCode code, ColumnSymbol column)
        {
            return GetSourceColumns(code, new[] { column });
        }

        /// <summary>
        /// Returns the set of database table columns that contributed to the data contained in the specified columns.
        /// </summary>
        public static IReadOnlyList<ColumnSymbol> GetSourceColumns(this KustoCode code, IReadOnlyList<ColumnSymbol> columns)
        {
            return GetSourceColumns(columns, code.Globals);
        }

        /// <summary>
        /// Returns the a map between the query's result columns and the set of database table columns that contributed to them.
        /// </summary>
        public static IReadOnlyDictionary<ColumnSymbol, IReadOnlyList<ColumnSymbol>> GetSourceColumnMap(this KustoCode code)
        {
            return GetSourceColumnMap(code, GetResultColumns(code));
        }

        /// <summary>
        /// Returns the a map between specified result columns and the set of database table columns that contributed to them.
        /// </summary>
        private static IReadOnlyDictionary<ColumnSymbol, IReadOnlyList<ColumnSymbol>> GetSourceColumnMap(
            this KustoCode code, IReadOnlyList<ColumnSymbol> columns)
        {
            return GetSourceColumnMap(columns, code.Globals);
        }

        /// <summary>
        /// Returns source columns with function call chain attribution for all result columns.
        /// Each source column includes the chain of function calls traversed to reach it.
        /// </summary>
        public static IReadOnlyList<SourceColumnInfo> GetSourceColumnsWithFunctions(this KustoCode code)
        {
            return GetSourceColumnsWithFunctions(code, GetResultColumns(code));
        }

        /// <summary>
        /// Returns source columns with function call chain attribution for the specified column.
        /// </summary>
        public static IReadOnlyList<SourceColumnInfo> GetSourceColumnsWithFunctions(this KustoCode code, ColumnSymbol column)
        {
            return GetSourceColumnsWithFunctions(code, new[] { column });
        }

        /// <summary>
        /// Returns source columns with function call chain attribution for the specified columns.
        /// </summary>
        public static IReadOnlyList<SourceColumnInfo> GetSourceColumnsWithFunctions(this KustoCode code, IReadOnlyList<ColumnSymbol> columns)
        {
            var expansionMap = new Dictionary<SyntaxElement, ExpansionInfo>();
            var tableRefMap = new Dictionary<TableSymbol, List<List<FunctionSymbol>>>();
            BuildExpansionMap(code.Syntax, code.Globals, expansionMap, tableRefMap: tableRefMap);

            var resultList = new List<SourceColumnInfo>();
            var seen = new HashSet<(ColumnSymbol, string)>();

            foreach (var c in columns)
            {
                GatherSourceColumnsWithFunctionsFromColumn(c, code.Globals, resultList, seen, expansionMap, tableRefMap);
            }

            return resultList;
        }

        /// <summary>
        /// Returns a map between result columns and their source columns with function attribution.
        /// </summary>
        public static IReadOnlyDictionary<ColumnSymbol, IReadOnlyList<SourceColumnInfo>> GetSourceColumnMapWithFunctions(this KustoCode code)
        {
            return GetSourceColumnMapWithFunctions(code, GetResultColumns(code));
        }

        /// <summary>
        /// Returns a map between specified columns and their source columns with function attribution.
        /// </summary>
        public static IReadOnlyDictionary<ColumnSymbol, IReadOnlyList<SourceColumnInfo>> GetSourceColumnMapWithFunctions(
            this KustoCode code, IReadOnlyList<ColumnSymbol> columns)
        {
            var expansionMap = new Dictionary<SyntaxElement, ExpansionInfo>();
            var tableRefMap = new Dictionary<TableSymbol, List<List<FunctionSymbol>>>();
            BuildExpansionMap(code.Syntax, code.Globals, expansionMap, tableRefMap: tableRefMap);

            var columnMap = new Dictionary<ColumnSymbol, IReadOnlyList<SourceColumnInfo>>();

            foreach (var c in columns)
            {
                var resultList = new List<SourceColumnInfo>();
                var seen = new HashSet<(ColumnSymbol, string)>();

                GatherSourceColumnsWithFunctionsFromColumn(c, code.Globals, resultList, seen, expansionMap, tableRefMap);

                columnMap.Add(c, resultList.ToReadOnly());
            }

            return columnMap;
        }

        /// <summary>
        /// Returns the a map between the specified columns and the set of database table columns that contributed to them.
        /// </summary>
        private static IReadOnlyList<ColumnSymbol> GetSourceColumns(
            IReadOnlyList<ColumnSymbol> columns, GlobalState globals)
        {
            var columnSet = new HashSet<ColumnSymbol>();
            var columnList = new List<ColumnSymbol>();

            foreach (var c in columns)
            {
                GatherSourceColumnsFromColumn(c, globals, columnSet, columnList);
            }

            return columnList;
        }

        /// <summary>
        /// Returns the a map between specified result columns and the set of database table columns that contributed to them.
        /// </summary>
        private static IReadOnlyDictionary<ColumnSymbol, IReadOnlyList<ColumnSymbol>> GetSourceColumnMap(
            IReadOnlyList<ColumnSymbol> columns, GlobalState globals)
        {
            var columnMap = new Dictionary<ColumnSymbol, IReadOnlyList<ColumnSymbol>>();
            var columnSet = new HashSet<ColumnSymbol>();
            var columnList = new List<ColumnSymbol>();

            foreach (var c in columns)
            {
                columnSet.Clear();
                columnList.Clear();

                GatherSourceColumnsFromColumn(c, globals, columnSet, columnList);

                columnMap.Add(c, columnList.ToReadOnly());
            }

            return columnMap;
        }

        /// <summary>
        /// Returns source columns with function attribution for the specified columns.
        /// </summary>
        private static IReadOnlyList<SourceColumnInfo> GetSourceColumnsWithFunctions(
            IReadOnlyList<ColumnSymbol> columns, GlobalState globals,
            Dictionary<SyntaxElement, ExpansionInfo> expansionMap,
            Dictionary<TableSymbol, List<List<FunctionSymbol>>> tableRefMap)
        {
            var resultList = new List<SourceColumnInfo>();
            var seen = new HashSet<(ColumnSymbol, string)>();

            foreach (var c in columns)
            {
                GatherSourceColumnsWithFunctionsFromColumn(c, globals, resultList, seen, expansionMap, tableRefMap);
            }

            return resultList;
        }

        /// <summary>
        /// Returns a map between specified result columns and their source columns with function attribution.
        /// </summary>
        private static IReadOnlyDictionary<ColumnSymbol, IReadOnlyList<SourceColumnInfo>> GetSourceColumnMapWithFunctions(
            IReadOnlyList<ColumnSymbol> columns, GlobalState globals,
            Dictionary<SyntaxElement, ExpansionInfo> expansionMap,
            Dictionary<TableSymbol, List<List<FunctionSymbol>>> tableRefMap)
        {
            var columnMap = new Dictionary<ColumnSymbol, IReadOnlyList<SourceColumnInfo>>();

            foreach (var c in columns)
            {
                var resultList = new List<SourceColumnInfo>();
                var seen = new HashSet<(ColumnSymbol, string)>();

                GatherSourceColumnsWithFunctionsFromColumn(c, globals, resultList, seen, expansionMap, tableRefMap);

                columnMap.Add(c, resultList.ToReadOnly());
            }

            return columnMap;
        }

        /// <summary>
        /// Gathers a list of database table columns that are the source of the data in the specified column.
        /// </summary>
        private static void GatherSourceColumnsFromColumn(
            ColumnSymbol column,
            GlobalState globals,
            HashSet<ColumnSymbol> columnSet,
            List<ColumnSymbol> columnList)
        {
            var processedColumns = s_columnSetPool.AllocateFromPool();
            var processedExprs = s_nodeSetPool.AllocateFromPool();
            var columnQueue = s_columnQueuePool.AllocateFromPool();
            var exprQueue = s_nodeQueuePool.AllocateFromPool();

            try
            {
                columnQueue.Enqueue(column);

                while (columnQueue.Count > 0)
                {
                    var c = columnQueue.Dequeue();

                    if (!processedColumns.Add(c))
                        continue;

                    if (GetTableOrView(globals, c) != null)
                    {
                        // This is a database table/materialized view/external table column.
                        if (columnSet.Add(c))
                            columnList.Add(c);
                    }
                    else if (c.OriginalColumns.Count > 0)
                    {
                        // Columns that have original-columns were created by semantic analysis
                        // to merge multiple columns into one (example: join and union operations).
                        foreach (var oc in c.OriginalColumns)
                        {
                            columnQueue.Enqueue(oc);
                        }
                    }
                    else if (c.Source is Expression source)
                    {
                        // Columns with source expressions were declared and initialized by some expression within the query
                        // (examples: named projection expressions, function call arguments)
                        exprQueue.Enqueue(source);

                        while (exprQueue.Count > 0)
                        {
                            var node = exprQueue.Dequeue();

                            if (!processedExprs.Add(node))
                                continue;

                            SyntaxElement.WalkNodes(node,
                                fnBefore: n =>
                                {
                                    if (n is Expression e)
                                    {
                                        if (e.ReferencedSymbol is ColumnSymbol refColumn)
                                        {
                                            columnQueue.Enqueue(refColumn);
                                        }
                                        else if (e.ReferencedSymbol is VariableSymbol variable
                                            && variable.Source is Expression varSource)
                                        {
                                            exprQueue.Enqueue(varSource);
                                        }
                                        else if (e.GetCalledFunctionBody() is SyntaxNode body)
                                        {
                                            exprQueue.Enqueue(body);
                                        }
                                    }
                                },
                                fnAfter: n =>
                                {
                                    if (n.Alternates != null)
                                    {
                                        foreach (var alt in n.Alternates)
                                        {
                                            exprQueue.Enqueue(alt);
                                        }
                                    }
                                },
                                // don't look inside function declarations, we already do this when we recurse into called function bodies.
                                fnDescend: n => !(n is FunctionDeclaration)
                                );
                        }
                    }
                }
            }
            finally
            {
                s_columnSetPool.ReturnToPool(processedColumns);
                s_nodeSetPool.ReturnToPool(processedExprs);
                s_columnQueuePool.ReturnToPool(columnQueue);
                s_nodeQueuePool.ReturnToPool(exprQueue);
            }
        }

        /// <summary>
        /// Records which FunctionSymbol created an expansion tree and from which tree it was called.
        /// </summary>
        private class ExpansionInfo
        {
            public FunctionSymbol Function;
            public SyntaxElement CallerRoot;

            public ExpansionInfo(FunctionSymbol function, SyntaxElement callerRoot)
            {
                Function = function;
                CallerRoot = callerRoot;
            }
        }

        /// <summary>
        /// Gets the root element of a syntax subtree by walking up Parent references.
        /// </summary>
        private static SyntaxElement GetSyntaxRoot(SyntaxElement element)
        {
            while (element.Parent != null)
                element = element.Parent;
            return element;
        }

        /// <summary>
        /// Pre-scans the syntax tree (and all expansion trees recursively) to build:
        /// 1. An expansion map: each expansion root → (FunctionSymbol, callerRoot)
        /// 2. A table reference map: each database table → list of function chains under which it was referenced
        /// Together these enable function attribution for both renamed and pass-through columns.
        /// </summary>
        private static void BuildExpansionMap(
            SyntaxNode root,
            GlobalState globals,
            Dictionary<SyntaxElement, ExpansionInfo> map,
            HashSet<SyntaxNode> visited = null,
            Dictionary<TableSymbol, List<List<FunctionSymbol>>> tableRefMap = null)
        {
            if (visited == null)
                visited = new HashSet<SyntaxNode>();
            if (!visited.Add(root))
                return;

            var callerRoot = GetSyntaxRoot(root);

            SyntaxElement.WalkNodes(root,
                fnBefore: n =>
                {
                    // Record table references for the table reference map
                    if (tableRefMap != null && n is Expression tableExpr
                        && tableExpr.ReferencedSymbol is TableSymbol refTable
                        && globals.GetDatabase(refTable) != null)
                    {
                        var chain = ComputeFunctionChain(n, map);
                        if (!tableRefMap.TryGetValue(refTable, out var chains))
                        {
                            chains = new List<List<FunctionSymbol>>();
                            tableRefMap[refTable] = chains;
                        }
                        // Only add if this chain signature is not already recorded
                        var chainKey = GetChainKey(chain);
                        if (!chains.Any(c => GetChainKey(c) == chainKey))
                        {
                            chains.Add(chain);
                        }
                    }

                    if (n is Expression e && e.GetCalledFunctionBody() is SyntaxNode body)
                    {
                        var bodyRoot = GetSyntaxRoot(body);
                        if (!map.ContainsKey(bodyRoot))
                        {
                            if (e.ReferencedSymbol is FunctionSymbol fs
                                && globals.GetDatabase(fs) != null)
                            {
                                map[bodyRoot] = new ExpansionInfo(fs, callerRoot);
                            }
                        }
                        // Recurse into the expansion body to find nested function calls
                        // (and table references within those expansions)
                        BuildExpansionMap(body, globals, map, visited, tableRefMap);
                    }
                },
                fnAfter: n =>
                {
                    if (n.Alternates != null)
                    {
                        foreach (var alt in n.Alternates)
                        {
                            BuildExpansionMap(alt, globals, map, visited, tableRefMap);
                        }
                    }
                },
                fnDescend: n => !(n is FunctionDeclaration));
        }

        /// <summary>
        /// Computes the function call chain for a node by walking up the expansion hierarchy.
        /// Returns the chain ordered outermost-first (index 0 = closest to query, last = closest to table).
        /// </summary>
        private static List<FunctionSymbol> ComputeFunctionChain(
            SyntaxElement node,
            Dictionary<SyntaxElement, ExpansionInfo> expansionMap)
        {
            var chain = new List<FunctionSymbol>();
            if (node == null)
                return chain;

            var root = GetSyntaxRoot(node);
            while (expansionMap.TryGetValue(root, out var info))
            {
                chain.Insert(0, info.Function);
                root = info.CallerRoot;
            }
            return chain;
        }

        /// <summary>
        /// BFS queue item that carries function call chain context through expression traversal.
        /// </summary>
        private struct ExprWithFunctions
        {
            public SyntaxNode Node;
            public List<FunctionSymbol> FunctionChain;

            public ExprWithFunctions(SyntaxNode node, List<FunctionSymbol> functionChain)
            {
                Node = node;
                FunctionChain = functionChain;
            }
        }

        /// <summary>
        /// BFS queue item that carries function call chain context through column traversal.
        /// </summary>
        private struct ColumnWithFunctions
        {
            public ColumnSymbol Column;
            public List<FunctionSymbol> FunctionChain;

            public ColumnWithFunctions(ColumnSymbol column, List<FunctionSymbol> functionChain)
            {
                Column = column;
                FunctionChain = functionChain;
            }
        }

        /// <summary>
        /// Gathers source columns with function call chain attribution.
        /// Uses a pre-built expansion map to determine which function created each expansion tree,
        /// and a table reference map to recover function attribution for pass-through columns
        /// (where the binder reuses the same ColumnSymbol instances from the underlying table).
        /// </summary>
        private static void GatherSourceColumnsWithFunctionsFromColumn(
            ColumnSymbol column,
            GlobalState globals,
            List<SourceColumnInfo> resultList,
            HashSet<(ColumnSymbol, string)> seen,
            Dictionary<SyntaxElement, ExpansionInfo> expansionMap,
            Dictionary<TableSymbol, List<List<FunctionSymbol>>> tableRefMap = null)
        {
            var processedColumns = new HashSet<(ColumnSymbol, string)>();
            var processedExprs = new HashSet<(SyntaxNode, string)>();
            var columnQueue = new Queue<ColumnWithFunctions>();
            var exprQueue = new Queue<ExprWithFunctions>();

            // Compute initial chain: check if the column's Source lives inside an expansion
            var initialChain = column.Source is SyntaxNode src
                ? ComputeFunctionChain(src, expansionMap)
                : new List<FunctionSymbol>();

            columnQueue.Enqueue(new ColumnWithFunctions(column, initialChain));

            while (columnQueue.Count > 0)
            {
                var item = columnQueue.Dequeue();
                var c = item.Column;
                var chain = item.FunctionChain;
                var chainKey = GetChainKey(chain);

                if (!processedColumns.Add((c, chainKey)))
                    continue;

                if (GetTableOrView(globals, c) != null)
                {
                    // Terminal: database table/materialized view/external table column.
                    // If BFS arrived with an empty chain, this may be a pass-through column
                    // (where/take/sort/top reuse the same ColumnSymbol). Consult the table
                    // reference map to recover function attribution from the syntax tree.
                    if (chain.Count == 0 && tableRefMap != null)
                    {
                        var table = GetTableOrView(globals, c);
                        if (table != null && tableRefMap.TryGetValue(table, out var tableChains))
                        {
                            // Emit one result per distinct non-empty function chain
                            bool foundFunctionChain = false;
                            foreach (var tableChain in tableChains)
                            {
                                if (tableChain.Count > 0)
                                {
                                    var tcKey = (c, GetChainKey(tableChain));
                                    if (seen.Add(tcKey))
                                    {
                                        resultList.Add(new SourceColumnInfo(c, tableChain.ToArray()));
                                    }
                                    foundFunctionChain = true;
                                }
                            }

                            // Also check if the table was referenced directly (empty chain)
                            // — emit the direct entry too so both attributions are reported
                            bool hasDirectRef = tableChains.Any(tc => tc.Count == 0);
                            if (hasDirectRef || !foundFunctionChain)
                            {
                                var key = (c, chainKey);
                                if (seen.Add(key))
                                {
                                    resultList.Add(new SourceColumnInfo(c, chain.ToArray()));
                                }
                            }
                            continue;
                        }
                    }

                    // Default: emit with current chain (non-empty chain or no table ref map)
                    {
                        var key = (c, chainKey);
                        if (seen.Add(key))
                        {
                            resultList.Add(new SourceColumnInfo(c, chain.ToArray()));
                        }
                    }
                }
                else if (c.OriginalColumns.Count > 0)
                {
                    // Merged column (union/join) — pass current chain to each original.
                    foreach (var oc in c.OriginalColumns)
                    {
                        columnQueue.Enqueue(new ColumnWithFunctions(oc, chain));
                    }
                }
                else if (c.Source is Expression source)
                {
                    // Column declared by an expression. Compute the chain from
                    // the source expression's expansion context — this detects the
                    // outermost function boundary that the BFS wouldn't otherwise see.
                    var sourceChain = ComputeFunctionChain(source, expansionMap);

                    exprQueue.Enqueue(new ExprWithFunctions(source, sourceChain));

                    while (exprQueue.Count > 0)
                    {
                        var exprItem = exprQueue.Dequeue();
                        var node = exprItem.Node;
                        var exprChain = exprItem.FunctionChain;
                        var exprChainKey = GetChainKey(exprChain);

                        if (!processedExprs.Add((node, exprChainKey)))
                            continue;

                        SyntaxElement.WalkNodes(node,
                            fnBefore: n =>
                            {
                                if (n is Expression e)
                                {
                                    if (e.ReferencedSymbol is ColumnSymbol refColumn)
                                    {
                                        columnQueue.Enqueue(new ColumnWithFunctions(refColumn, exprChain));
                                    }
                                    else if (e.ReferencedSymbol is VariableSymbol variable
                                        && variable.Source is Expression varSource)
                                    {
                                        exprQueue.Enqueue(new ExprWithFunctions(varSource, exprChain));
                                    }
                                    else if (e.GetCalledFunctionBody() is SyntaxNode body)
                                    {
                                        // Compute chain for the body from its expansion context.
                                        var bodyChain = ComputeFunctionChain(body, expansionMap);
                                        exprQueue.Enqueue(new ExprWithFunctions(body, bodyChain));
                                    }
                                }
                            },
                            fnAfter: n =>
                            {
                                if (n.Alternates != null)
                                {
                                    foreach (var alt in n.Alternates)
                                    {
                                        exprQueue.Enqueue(new ExprWithFunctions(alt, exprChain));
                                    }
                                }
                            },
                            fnDescend: n => !(n is FunctionDeclaration)
                            );
                    }
                }
            }
        }

        /// <summary>
        /// Gets the database table (or materialized view / external table) that contains the column.
        /// GlobalState.GetTable only indexes database.Tables which excludes materialized views and
        /// external tables. This helper falls back to scanning those collections when needed.
        /// </summary>
        private static TableSymbol GetTableOrView(GlobalState globals, ColumnSymbol column)
        {
            // Fast path: regular table columns are indexed in the reverse map
            var table = globals.GetTable(column);
            if (table != null)
                return table;

            // Slow path: scan materialized views and external tables across all databases.
            // This is needed because GlobalState.GetTable() builds its reverse map from
            // database.Tables, which explicitly filters out IsMaterializedView and IsExternal.
            foreach (var database in globals.Clusters.SelectMany(c => c.Databases))
            {
                foreach (var mv in database.MaterializedViews)
                {
                    if (mv.Columns.Contains(column))
                        return mv;
                }

                foreach (var et in database.ExternalTables)
                {
                    if (et.Columns.Contains(column))
                        return et;
                }
            }

            return null;
        }

        /// <summary>
        /// Creates a string key for a function chain to use in deduplication sets.
        /// </summary>
        private static string GetChainKey(List<FunctionSymbol> chain)
        {
            if (chain.Count == 0)
                return "";
            return string.Join(">", chain.Select(f => f.Name));
        }
    }
}
