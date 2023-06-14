using System;
using System.Collections.Generic;
using System.Linq;
using wooby.Error;

namespace wooby.Parsing
{
    public partial class Parser
    {
        private static void ResolveSelectReferences(SelectStatement statement, Context context)
        {
            foreach (var expr in statement.OutputColumns)
            {
                if (expr.Nodes.Any(n => n.Kind == Expression.NodeKind.Reference || n.Kind == Expression.NodeKind.Function || n.Kind == Expression.NodeKind.SubSelect))
                {
                    ResolveUnresolvedReferences(expr, context, statement);
                }
            }

            if (statement.FilterConditions != null)
            {
                ResolveUnresolvedReferences(statement.FilterConditions, context, statement);
            }

            if (statement.Parent != null)
            {
                foreach (var order in statement.OutputOrder)
                {
                    ResolveUnresolvedReferences(order.OrderExpression, context, statement);
                }
            }
        }

        private static void AssertGroupingIsCorrect(SelectStatement query)
        {
            // Verify that when using a group by clause, only group by expressions are referenced in the output

            if (query.Grouping.Count > 0)
            {
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
                        foreach (var node in output.Nodes)
                        {
                            if (node.Kind == Expression.NodeKind.Function && node.FunctionCall.CalledVariant.IsAggregate)
                            {
                                foreach (var parameter in node.FunctionCall.Arguments)
                                {
                                    if (query.Grouping.Contains(parameter))
                                    {
                                        throw new WoobyParserException(
                                            "GROUP BY expression used as argument for aggregate function", 0);
                                    }
                                }
                            }
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
        }

        private static void ExpandWildcards(Context context, SelectStatement query)
        {
            if (query.OutputColumns.Any(e => e.IsWildcard()))
            {
                var newOutput = new List<Expression>();

                foreach (var expr in query.OutputColumns)
                {
                    if (expr.IsWildcard())
                    {
                        if (expr.IsOnlyReference())
                        {
                            var wild = expr.Nodes[0].ReferenceValue;
                            var table = context.FindTable(wild);

                            foreach (var col in table.Columns)
                            {
                                var reference = new ColumnReference { Column = col.Name, Table = table.Name };
                                var node = new Expression.Node { Kind = Expression.NodeKind.Reference, ReferenceValue = reference };
                                newOutput.Add(Expression.WithSingleNode(node, Expression.ExpressionType.Unknown, reference.Join()));
                            }
                        } else
                        {
                            // TODO: Should bring all tables
                            var table = context.FindTable(query.MainSource);
                            if (table == null)
                            {
                                throw new WoobyParserException($"No such table {query.MainSource}", 0);
                            }

                            foreach (var col in table.Columns)
                            {
                                var reference = new ColumnReference { Column = col.Name, Table = table.Name };
                                var node = new Expression.Node { Kind = Expression.NodeKind.Reference, ReferenceValue = reference };
                                var colExpr = Expression.WithSingleNode(node, Expression.ExpressionType.Unknown, reference.Join());
                                colExpr.Identifier = col.Name;
                                newOutput.Add(colExpr);
                            }
                        }
                    } else
                    {
                        newOutput.Add(expr);
                    }
                }

                query.OutputColumns = newOutput;
            }
        }

        public SelectStatement ParseSelect(string input, int offset, Context context, StatementFlags flags, Statement parent)
        {
            int originalOffset = offset;
            var statement = new SelectStatement();
            if (parent != null && parent is SelectStatement)
            {
                statement.Parent = parent;
            }
            statement.UsedFlags = flags;

            // First token is SELECT
            SkipNextToken(input, ref offset);
            var exprFlags = new ExpressionFlags { GeneralWildcardAllowed = true, IdentifierAllowed = true, WildcardAllowed = true, SingleValueSubSelectAllowed = true, AllowAggregateFunctions = true };

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
                    } else if (next.KeywordValue == Keyword.Distinct)
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
                    throw new WoobyParserException("Unrecognized token at start of expression in output definition", offset, next);
                }
                var expr = ParseExpression(input, offset, context, statement, exprFlags, false, false);
                if (statement.OutputColumns.Count > 0 && expr.IsWildcard() && !expr.IsOnlyReference())
                {
                    throw new WoobyParserException("Unexpected token *", offset);
                }

                statement.OutputColumns.Add(expr);
                offset += expr.FullText.Length;

                // Disallow general wildflags after first column
                exprFlags.GeneralWildcardAllowed = false;
            } while (true);

            var source = ParseReference(input, offset, context, statement, new ReferenceFlags() { TableOnly = true });
            statement.MainSource = source;
            offset += source.InputLength;

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

                        bool firstOrder = true;

                        while (true)
                        {
                            next = NextToken(input, offset);

                            if (next.Kind == TokenKind.Comma)
                            {
                                if (!firstOrder)
                                {
                                    offset += next.InputLength;
                                } else
                                {
                                    throw new WoobyParserException("Order by clause cannot start with a comma", offset);
                                }
                            } else if (next.Kind == TokenKind.Operator && next.OperatorValue == Operator.ParenthesisRight)
                            {
                                if (flags.StopOnUnmatchedParenthesis)
                                {
                                    offset += next.InputLength;
                                    break;
                                } else
                                {
                                    throw new WoobyParserException("Missing left parenthesis", offset, next);
                                }
                            } else if (next.Kind == TokenKind.Keyword || next.Kind == TokenKind.None)
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
                                OrderExpression = ParseExpression(input, offset, context, statement, new ExpressionFlags { SingleValueSubSelectAllowed = true }, parent == null, false)
                            };
                            offset += ordering.OrderExpression.FullText.Length;

                            next = NextToken(input, offset);
                            if (next.Kind == TokenKind.Keyword && (next.KeywordValue == Keyword.Asc || next.KeywordValue == Keyword.Desc))
                            {
                                ordering.Kind = next.KeywordValue == Keyword.Asc ? OrderingKind.Ascending : OrderingKind.Descending;
                                offset += next.InputLength;
                            }

                            statement.OutputOrder.Add(ordering);
                        }
                    } else if (next.KeywordValue == Keyword.Group)
                    {
                        next = NextToken(input, offset);
                        offset += next.InputLength;
                        AssertTokenIsKeyword(next, Keyword.By, "Unexpected BY after GROUP");

                        while (true)
                        {
                            next = NextToken(input, offset);

                            if (next.Kind == TokenKind.Comma)
                            {
                                if (statement.Grouping.Count == 0)
                                {
                                    throw new WoobyParserException("Expected column name after GROUP BY", offset, next);
                                } else
                                {
                                    offset += next.InputLength;
                                }
                            } else if (next.Kind == TokenKind.None || next.Kind == TokenKind.Keyword)
                            {
                                break;
                            }

                            var expr = ParseExpression(input, offset, context, statement, new ExpressionFlags(), true, false);
                            statement.Grouping.Add(expr);
                            offset += expr.FullText.Length;
                        }
                    }
                }
                else if (next.Kind == TokenKind.Operator && next.OperatorValue == Operator.ParenthesisRight)
                {
                    if (!flags.StopOnUnmatchedParenthesis)
                    {
                        offset += next.InputLength;
                        break;
                    }
                    else
                    {
                        throw new WoobyParserException("Missing left parenthesis", offset, next);
                    }
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

            offset = Math.Min(input.Length, offset);
            statement.OriginalText = input[originalOffset..offset];

            return statement;
        }
    }
}
