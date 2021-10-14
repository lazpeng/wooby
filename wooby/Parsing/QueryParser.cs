using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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

            if (statement.Parent != null)
            {
                ResolveUnresolvedReferences(statement.FilterConditions, context, statement);

                foreach (var order in statement.OutputOrder)
                {
                    ResolveUnresolvedReferences(order.OrderExpression, context, statement);
                }
            }
        }

        private static void AssertGroupingIsCorrect(SelectStatement query)
        {
            // Verify that when using a group by clause, only group by-columns are referenced in the output

            if (query.Grouping.Count > 0)
            {
                if (query.OutputColumns.Any(o => o.IsWildcard()))
                {
                    // Not sure if this is correct
                    throw new Exception("Invalid wildcard when using group by clause");
                }

                foreach (var output in query.OutputColumns)
                {
                    foreach (var node in output.Nodes)
                    {
                        if (node.Kind == Expression.NodeKind.Reference)
                        {
                            // TODO: Add aggregate functions
                            // Verify that the used reference is present on the group by clause
                            if (!query.Grouping.Contains(node.ReferenceValue))
                            {
                                throw new Exception("Referencing column not present in group by clause");
                            }
                        }
                    }
                }
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

            Token next = null;

            do
            {
                next = NextToken(input, offset);
                if (next.Kind == TokenKind.Keyword && next.KeywordValue == Keyword.From)
                {
                    offset += next.InputLength;
                    break;
                }
                else if (next.Kind == TokenKind.None)
                {
                    throw new Exception("Unexpected end of input");
                }
                else if (next.Kind == TokenKind.Comma)
                {
                    if (statement.OutputColumns.Count == 0)
                    {
                        throw new Exception("Query starts with a comma");
                    }
                    offset += next.InputLength;
                }

                var expr = ParseExpression(input, offset, context, statement, exprFlags, false, false);

                if (statement.OutputColumns.Count > 0 && expr.IsWildcard() && !expr.IsOnlyReference())
                {
                    throw new Exception("Unexpected token *");
                }

                statement.OutputColumns.Add(expr);
                offset += expr.FullText.Length;

                // Disallow general wildflags after first column
                exprFlags.GeneralWildcardAllowed = false;
            } while (true);

            var source = ParseReference(input, offset, context, statement, new ReferenceFlags() { TableOnly = true });
            statement.MainSource = source;
            offset += source.InputLength;

            // If we have a parent, they're going to resolve our references for us
            if (parent == null)
            {
                ResolveSelectReferences(statement, context);
            }

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
                                    throw new Exception("Order by clause cannot start with a comma");
                                }
                            } else if (next.Kind == TokenKind.Operator && next.OperatorValue == Operator.ParenthesisRight)
                            {
                                if (flags.StopOnUnmatchedParenthesis)
                                {
                                    offset += next.InputLength;
                                    break;
                                } else
                                {
                                    throw new Exception("Missing left parenthesis");
                                }
                            } else if (next.Kind == TokenKind.Keyword || next.Kind == TokenKind.None)
                            {
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
                                    throw new Exception("Expected column name after ORDER BY");
                                } else
                                {
                                    offset += next.InputLength;
                                }
                            } else if (next.Kind == TokenKind.None)
                            {
                                break;
                            }

                            var reference = ParseReference(input, offset, context, statement, new ReferenceFlags { ResolveReferences = true });
                            statement.Grouping.Add(reference);
                            offset += reference.InputLength;
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
                        throw new Exception("Missing left parenthesis");
                    }
                }
                else if (next.Kind != TokenKind.None)
                {
                    throw new Exception($"Unexpected token in query at offset {offset}");
                }
            } while (next.Kind != TokenKind.None && next.Kind != TokenKind.SemiColon);

            AssertGroupingIsCorrect(statement);

            offset = Math.Min(input.Length, offset);
            statement.OriginalText = input[originalOffset..offset];

            return statement;
        }
    }
}
