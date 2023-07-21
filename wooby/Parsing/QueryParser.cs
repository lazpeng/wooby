using System;
using System.Collections.Generic;
using System.Linq;
using wooby.Error;

namespace wooby.Parsing;

public partial class Parser
{
    private static readonly Dictionary<Keyword, JoinKind> KeywordToJoinKind = new()
    {
        [Keyword.Inner] = JoinKind.Inner,
        [Keyword.Left] = JoinKind.Left,
        [Keyword.Right] = JoinKind.Right
    };

    private static void ResolveSelectReferences(SelectStatement statement, Context context)
    {
        foreach (var expr in statement.OutputColumns.Where(expr => expr.Nodes.Any(n =>
                     n.Kind is Expression.NodeKind.Reference or Expression.NodeKind.Function
                         or Expression.NodeKind.SubSelect)))
        {
            ResolveUnresolvedReferences(expr, context, statement);
        }

        foreach (var source in statement.Sources)
        {
            ResolveUnresolvedReferences(source.Condition, context, statement);
        }

        if (statement.Parent == null) return;
        foreach (var order in statement.OutputOrder)
        {
            ResolveUnresolvedReferences(order.OrderExpression, context, statement);
        }
    }

    private static void AssertJoinsAreCorrect(Statement statement)
    {
        // Check alias clashes
        // check all tables with the same name have an alias
        // check all references point to a valid table and alias

        var repeatedTables = statement.Sources.Select(s => s.Source.Table).GroupBy(g => g).Where(g => g.Count() > 1).Select(g => g.Key);
        foreach (var table in repeatedTables)
        {
            var repeated = statement.Sources.Select(s => s.Source).Where(t => t.Table == table);
            // Any of the table references does not have an alias or has an duplicated one
            if (repeated.Select(r => r.Identifier).GroupBy(g => g)
                .Any(g => g.Count() != 1 || string.IsNullOrEmpty(g.Key)))
            {
                throw new WoobyParserException("Same table appear more than once as a source without distinct aliases",
                    0);
            }
        }

        // Check if any aliases are repeated
        if (statement.Sources.Select(s => s.Source.Identifier).Where(s => !string.IsNullOrEmpty(s)).GroupBy(s => s)
            .Any(g => g.Count() > 1))
        {
            throw new WoobyParserException("Same alias used for more than one table in statement", 0);
        }
        
        // Check that all joins have a condition (except for the main [0] source)
        if (statement.Sources.Skip(1).Any(s => !s.Condition.Nodes.Any()))
        {
            throw new WoobyParserException("A JOIN clause must have a valid ON condition", 0);
        }
    }

    private static void AssertGroupingIsCorrect(SelectStatement query)
    {
        // Verify that when using a group by clause, only group by expressions are referenced in the output

        if (query.Grouping.Count <= 0) return;
        if (query.OutputColumns.Any(o => o.IsWildcard()))
        {
            // Not sure if this is correct
            throw new WoobyParserException("Invalid wildcard when using GROUP BY clause", 0);
        }

        foreach (var output in query.OutputColumns)
        {
            if (output.HasAggregateFunction)
            {
                // Make sure that the aggregate function does NOT use a column or expression in the group by
                if (output.Nodes
                    .Where(node => node is { Kind: Expression.NodeKind.Function, FunctionCall.CalledVariant.IsAggregate: true })
                    .Any(node => node.FunctionCall != null && node.FunctionCall.Arguments.Any(p => query.Grouping.Contains(p))))
                {
                    throw new WoobyParserException(
                        "GROUP BY expression used as argument for aggregate function", 0);
                }
            }
            else
            {
                // Else, make sure that the expression is present in the group by clause
                if (!query.Grouping.Contains(output))
                {
                    throw new WoobyParserException("Output expression not referenced in GROUP BY clause", 0);
                }
            }
        }
    }

    private static void ExpandSourceWildcards(Context context, TableSource source, ICollection<Expression> newOutput)
    {
        foreach (var col in source.GetMeta(context).Columns)
        {
            var reference = new ColumnReference { Column = col.Name, Table = source.CanonName };
            var node = new Expression.Node
                { Kind = Expression.NodeKind.Reference, ReferenceValue = reference };
            newOutput.Add(Expression.WithSingleNode(node, Expression.ExpressionType.Unknown, reference.Join()));
        }
    }

    private static void ExpandWildcards(Context context, SelectStatement query)
    {
        if (!query.OutputColumns.Any(e => e.IsWildcard())) return;
        var newOutput = new List<Expression>();

        foreach (var expr in query.OutputColumns)
        {
            if (expr.IsWildcard())
            {
                if (expr.IsOnlyReference() && !string.IsNullOrEmpty(expr.Nodes[0].ReferenceValue?.Table))
                {
                    var reference = expr.Nodes[0].ReferenceValue; 
                    if (reference == null)
                        throw new WoobyParserException("Internal error: reference is null", 0);
                    var name = reference.Table;
                    
                    var source = query.Sources.Select(j => j.Source)
                        .FirstOrDefault(s => s.NameMatches(name));

                    if (source == null)
                    {
                        throw new WoobyParserException($"Unable to expand wildcard for reference '{name}'", 0);
                    }

                    ExpandSourceWildcards(context, source, newOutput);
                }
                else
                {
                    foreach (var joins in query.Sources.Select(j => j.Source))
                    {
                        ExpandSourceWildcards(context, joins, newOutput);
                    }
                }
            }
            else
            {
                newOutput.Add(expr);
            }
        }

        query.OutputColumns = newOutput;
    }

    private SelectStatement ParseSelect(string input, int offset, Context context, StatementFlags flags, Statement? parent)
    {
        var originalOffset = offset;
        var statement = new SelectStatement();
        if (parent is SelectStatement)
        {
            statement.Parent = parent;
        }

        statement.UsedFlags = flags;

        if (flags.SkipFirstParenthesis)
        {
            SkipNextToken(input, ref offset);
        }

        // First token is SELECT
        SkipNextToken(input, ref offset);
        var exprFlags = new ExpressionFlags
        {
            GeneralWildcardAllowed = true, IdentifierAllowed = true, WildcardAllowed = true,
            SingleValueSubSelectAllowed = true, AllowAggregateFunctions = true
        };

        Token next;

        do
        {
            next = NextToken(input, offset);
            if (next.Kind == TokenKind.Keyword)
            {
                if (next.KeywordValue == Keyword.From)
                {
                    if (statement.OutputColumns.Count == 0)
                    {
                        throw new WoobyParserException("Expected output columns", offset, next);
                    }

                    offset += next.InputLength;
                    break;
                }
                if (next.KeywordValue == Keyword.Distinct)
                {
                    statement.Distinct = true;
                    offset += next.InputLength;
                }
                else
                {
                    throw new WoobyParserException("Unexpected keyword", offset, next);
                }
            }
            else if (next.Kind == TokenKind.Comma)
            {
                if (statement.OutputColumns.Count == 0)
                {
                    throw new WoobyParserException("Query starts with a comma", offset, next);
                }

                offset += next.InputLength;
            }
            else if (next.Kind == TokenKind.None)
            {
                throw new WoobyParserException("Unexpected end of input", offset);
            }

            next = NextToken(input, offset);
            if (Expression.IsTokenInvalidForExpressionStart(next))
            {
                throw new WoobyParserException("Unrecognized token at start of expression in output definition",
                    offset, next);
            }

            var expr = ParseExpression(input, offset, context, statement, exprFlags, false, false);
            if (statement.OutputColumns.Count > 0 && expr.IsWildcard() && !expr.IsOnlyReference())
            {
                throw new WoobyParserException("Unexpected token *", offset);
            }

            statement.OutputColumns.Add(expr);
            offset += expr.FullText.Length;

            // Disallow general wildcards after first column
            exprFlags.GeneralWildcardAllowed = false;
        } while (true);
        
        var mainSource = ParseTableSource(input, offset, context, statement);
        statement.Sources.Add(new Joining {Kind = JoinKind.Inner, Source = mainSource});
        offset += mainSource.InputLength;

        do
        {
            var joinKind = JoinKind.Inner;
            next = NextToken(input, offset);
            var explicitKind = false;

            if (next.Kind == TokenKind.Keyword && KeywordToJoinKind.TryGetValue(next.KeywordValue, out joinKind))
            {
                explicitKind = true;
                offset += next.InputLength;
            }

            var joinConditionFlags = new ExpressionFlags();

            next = NextToken(input, offset);
            if (next.Kind == TokenKind.Keyword)
            {
                if (next.KeywordValue == Keyword.Join)
                {
                    offset += next.InputLength;

                    var source = ParseTableSource(input, offset, context, statement);
                    offset += source.InputLength;

                    next = NextToken(input, offset);
                    AssertTokenIsKeyword(next, Keyword.On, "Expected ON after table JOIN");
                    offset += next.InputLength;

                    var condition = ParseExpression(input, offset, context, statement, joinConditionFlags, false,
                        false);
                    offset += condition.FullText.Length;

                    statement.Sources.Add(new Joining { Condition = condition, Kind = joinKind, Source = source });
                    ResolveUnresolvedReferences(condition, context, statement);
                }
                else if (explicitKind)
                {
                    throw new WoobyParserException("Expected JOIN", offset, next);
                }
                else break;
            }
            else break;
        } while (true);

        // FIXME: Enforce order of clauses
        do
        {
            next = NextToken(input, offset);
            offset += next.InputLength;

            if (next.Kind == TokenKind.Keyword)
            {
                if (next.KeywordValue == Keyword.Where)
                {
                    offset += ParseWhere(input, offset, context, statement);
                }
                else if (next.KeywordValue == Keyword.Order)
                {
                    next = NextToken(input, offset);
                    AssertTokenIsKeyword(next, Keyword.By, "Unexpected BY after ORDER");
                    offset += next.InputLength;

                    var firstOrder = true;

                    while (true)
                    {
                        next = NextToken(input, offset);

                        if (next.Kind == TokenKind.Comma)
                        {
                            if (!firstOrder)
                            {
                                offset += next.InputLength;
                            }
                            else
                            {
                                throw new WoobyParserException("Order by clause cannot start with a comma", offset);
                            }
                        }
                        else if (next is { Kind: TokenKind.Operator, OperatorValue: Operator.ParenthesisRight })
                        {
                            if (!flags.StopOnUnmatchedParenthesis)
                                throw new WoobyParserException("Missing left parenthesis", offset, next);
                            offset += next.InputLength;
                            break;
                        }
                        else if (next.Kind is TokenKind.Keyword or TokenKind.None)
                        {
                            if (firstOrder)
                            {
                                throw new WoobyParserException("Expected expression after ORDER BY", offset);
                            }

                            break;
                        }

                        firstOrder = false;

                        var ordering = new Ordering
                        {
                            OrderExpression = ParseExpression(input, offset, context, statement,
                                new ExpressionFlags { SingleValueSubSelectAllowed = true }, parent == null, false)
                        };
                        offset += ordering.OrderExpression.FullText.Length;

                        next = NextToken(input, offset);
                        if (next is { Kind: TokenKind.Keyword, KeywordValue: Keyword.Asc or Keyword.Desc })
                        {
                            ordering.Kind = next.KeywordValue == Keyword.Asc
                                ? OrderingKind.Ascending
                                : OrderingKind.Descending;
                            offset += next.InputLength;
                        }

                        statement.OutputOrder.Add(ordering);
                    }
                }
                else if (next.KeywordValue == Keyword.Group)
                {
                    next = NextToken(input, offset);
                    offset += next.InputLength;
                    AssertTokenIsKeyword(next, Keyword.By, "Unexpected BY after GROUP");

                    while (true)
                    {
                        next = NextToken(input, offset);

                        if (next.Kind is TokenKind.Comma)
                        {
                            if (statement.Grouping.Count == 0)
                            {
                                throw new WoobyParserException("Expected column name after GROUP BY", offset, next);
                            }
                            offset += next.InputLength;
                        }
                        else if (next.Kind is TokenKind.None or TokenKind.Keyword)
                        {
                            break;
                        }

                        var expr = ParseExpression(input, offset, context, statement, new ExpressionFlags(), true,
                            false);
                        statement.Grouping.Add(expr);
                        offset += expr.FullText.Length;
                    }
                }
            }
            else if (next is { Kind: TokenKind.Operator, OperatorValue: Operator.ParenthesisRight })
            {
                if (!flags.StopOnUnmatchedParenthesis)
                    throw new WoobyParserException("Missing left parenthesis", offset, next);
                offset += next.InputLength;
                break;
            }
            else if (next.Kind != TokenKind.None)
            {
                throw new WoobyParserException($"Unexpected token in query at offset {offset}", offset, next);
            }
        } while (next.Kind != TokenKind.None && next.Kind != TokenKind.SemiColon);

        // If we have a parent, they're going to resolve our references for us
        if (parent == null)
        {
            ResolveSelectReferences(statement, context);
        }

        AssertGroupingIsCorrect(statement);
        ExpandWildcards(context, statement);
        AssertJoinsAreCorrect(statement);

        offset = Math.Min(input.Length, offset);
        statement.OriginalText = input[originalOffset..offset];
        statement.InputLength = offset - originalOffset;

        return statement;
    }
}