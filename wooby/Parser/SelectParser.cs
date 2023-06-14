using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace wooby.Parsing
{
    public partial class Parser
    {
        public SelectStatement ParseSelect(string input, int offset, Context context)
        {
            int originalOffset = offset;
            var command = new SelectStatement();

            Token next = NextToken(input, offset);
            if (next.Kind != TokenKind.Keyword || next.KeywordValue != Keyword.Select)
            {
                throw new Exception("Failed initial check");
            }
            offset += next.InputLength;

            var exprFlags = new ExpressionFlags { GeneralWildcardAllowed = true, IdentifierAllowed = true, WildcardAllowed = true };

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
                    offset += next.InputLength;
                }

                var expr = ParseExpression(input, offset, context, exprFlags, false);

                if (command.OutputColumns.Count > 0 && expr.IsWildcard() && !expr.IsOnlyReference())
                {
                    throw new Exception("Unexpected token *");
                }

                command.OutputColumns.Add(expr);
                offset += expr.FullText.Length;

                // Disallow general wildflags after first column
                exprFlags.GeneralWildcardAllowed = false;
            } while (true);

            var source = ParseReference(input, offset, context, new ReferenceFlags() { TableOnly = true });
            CurrentSources.Add(source);

            foreach (var expr in command.OutputColumns.Where(e => e.Nodes.Any(n => n.Kind == Expression.NodeKind.Reference)))
            {
                ResolveUnresolvedReferences(expr, context);
            }

            command.MainSource = source;
            offset += source.InputLength;

            // Prepare flags for WHERE and ORDER BY expressions
            exprFlags.GeneralWildcardAllowed = false;
            exprFlags.WildcardAllowed = false;
            exprFlags.IdentifierAllowed = false;

            do
            {
                next = NextToken(input, offset);
                offset += next.InputLength;

                if (next.Kind == TokenKind.Keyword)
                {
                    if (next.KeywordValue == Keyword.Where)
                    {
                        if (command.FilterConditions != null)
                        {
                            throw new Exception("Unexpected WHERE when filter has already been set");
                        }

                        command.FilterConditions = ParseExpression(input, offset, context, exprFlags, true);
                        offset += command.FilterConditions.FullText.Length;
                    }
                    else if (next.KeywordValue == Keyword.Order)
                    {
                        next = NextToken(input, offset);
                        if (next.Kind != TokenKind.Keyword || next.KeywordValue != Keyword.By)
                        {
                            throw new Exception("Unexpected token after ORDER");
                        }

                        offset += next.InputLength;

                        var orderExpr = ParseExpression(input, offset, context, exprFlags, true);
                        command.OutputOrder = new Ordering { OrderExpression = orderExpr };
                        offset += orderExpr.FullText.Length;

                        next = NextToken(input, offset);
                        if (next.Kind == TokenKind.Keyword && (next.KeywordValue == Keyword.Asc || next.KeywordValue == Keyword.Desc))
                        {
                            command.OutputOrder.Kind = next.KeywordValue == Keyword.Asc ? OrderingKind.Ascending : OrderingKind.Descending;
                            offset += next.InputLength;
                        }
                    }
                }
                else if (next.Kind != TokenKind.None)
                {
                    throw new Exception($"Unexpected token in query at offset {offset}");
                }
            } while (next.Kind != TokenKind.None && next.Kind != TokenKind.SemiColon);

            offset = Math.Min(input.Length, offset);
            command.OriginalText = input[originalOffset..offset];

            return command;
        }
    }
}
