using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace wooby
{
    public class Parser
    {
        public enum TokenKind
        {
            LiteralNumber,
            LiteralString,
            Symbol,
            Keyword,
            Operator,
            Comma,
            SemiColon,
            Dot,
            None
        }

        private readonly Dictionary<string, Keyword> keywordDict = new()
        {
            { "SELECT", Keyword.Select },
            { "FROM", Keyword.From },
            { "WHERE", Keyword.Where },
            { "NOT", Keyword.Not },
            { "NULL", Keyword.Null },
            { "IS", Keyword.Is },
            { "TRUE", Keyword.True },
            { "FALSE", Keyword.False },
            { "AND", Keyword.And },
            { "OR", Keyword.Or },
            { "ASC", Keyword.Asc },
            { "DESC", Keyword.Desc },
            { "ORDER", Keyword.Order },
            { "BY", Keyword.By },
            { "AS", Keyword.As }
        };

        private readonly Dictionary<char, Operator> operatorDict = new()
        {
            { '+', Operator.Plus },
            { '-', Operator.Minus },
            { '/', Operator.ForwardSlash },
            { '*', Operator.Asterisk },
            { '(', Operator.ParenthesisLeft },
            { ')', Operator.ParenthesisRight },
            { '^', Operator.Power },
            { '<', Operator.LessThan },
            { '>', Operator.MoreThan },
            { '=', Operator.Equal }
        };

        public class Token
        {
            public TokenKind Kind;
            public string StringValue;
            public double NumberValue;
            public Keyword KeywordValue;
            public Operator OperatorValue;
            public int InputLength;
        }

        public class ExpressionFlags
        {
            // This is for any wildcard, including the syntax tablename.*
            public bool WildcardAllowed = false;
            // This is for the single * character, disallowed when a column has already been specified
            public bool GeneralWildcardAllowed = false;
            // Whether or not you can alias the expression to an identifier
            public bool IdentifierAllowed = false;
        }

        private TokenKind PeekToken(string input, int offset)
        {
            var first = input[offset];

            if (char.IsDigit(first))
            {
                return TokenKind.LiteralNumber;
            }
            else if (first == '\"' || first == '_' || char.IsLetter(first))
            {
                return TokenKind.Symbol;
            }
            else if (operatorDict.ContainsKey(first))
            {
                return TokenKind.Operator;
            }
            else return TokenKind.None;
        }

        private List<TableReference> CurrentSources { get; } = new List<TableReference>();

        private static int SkipWhitespace(string input, int offset)
        {
            int original = offset;

            foreach (var c in input[offset..])
            {
                if (!char.IsWhiteSpace(c))
                {
                    break;
                }

                ++offset;
            }

            return offset - original;
        }

        public Token NextToken(string input, int offset = 0)
        {
            if (offset >= input.Length)
                return new Token() { Kind = TokenKind.None };

            var skipped = SkipWhitespace(input, offset);

            var result = input[offset + skipped] switch
            {
                ',' => new Token { Kind = TokenKind.Comma, InputLength = 1 },
                ';' => new Token { Kind = TokenKind.SemiColon, InputLength = 1 },
                '.' => new Token { Kind = TokenKind.Dot, InputLength = 1 },
                '\'' => ParseString(input, offset),
                _ => PeekToken(input, offset + skipped) switch
                {
                    TokenKind.Symbol => ParseSymbol(input, offset),
                    TokenKind.LiteralNumber => ParseNumber(input, offset),
                    TokenKind.Operator => ParseOperator(input, offset),
                    _ => new Token() { Kind = TokenKind.None }
                }
            };

            return result;
        }

        private Token ParseSymbol(string input, int offset)
        {
            int originalOffset = offset;

            offset += SkipWhitespace(input, offset);
            int start = offset;

            foreach (var c in input[offset..])
            {
                if ((c == '\"' && originalOffset != offset) || char.IsWhiteSpace(c) || !(char.IsDigit(c) || char.IsLetter(c) || c == '_') || char.IsControl(c))
                {
                    break;
                }

                ++offset;
            }

            string symbol = input[start..offset];

            if (keywordDict.TryGetValue(symbol.ToUpper(), out Keyword keyword))
            {
                return new Token { Kind = TokenKind.Keyword, KeywordValue = keyword, InputLength = offset - originalOffset };
            }
            else
            {
                return new Token { Kind = TokenKind.Symbol, StringValue = symbol, InputLength = offset - originalOffset };
            }
        }

        private static Token ParseString(string input, int offset)
        {
            int original = offset;

            offset += SkipWhitespace(input, offset) + 1;
            int start = offset;

            var lastWasEscape = false;

            foreach (var c in input[offset..])
            {
                ++offset;

                if (lastWasEscape)
                {
                    lastWasEscape = false;
                }
                else if (c == '\\')
                {
                    lastWasEscape = true;
                }
                else if (c == '\'')
                {
                    break;
                }
            }

            return new Token { Kind = TokenKind.LiteralString, StringValue = input[start..(offset - 1)], InputLength = offset - original };
        }

        private static Token ParseNumber(string input, int offset)
        {
            double value = 0d;
            int fraction = 0, sciNot = -1;

            int original = offset;

            offset += SkipWhitespace(input, offset);

            foreach (var c in input[offset..])
            {
                if (c == '.')
                {
                    if (fraction > 0 || sciNot >= 0)
                    {
                        throw new Exception("Unexpected '.' in number literal");
                    }
                    else fraction = 1;
                }
                else if ('e' == c || 'E' == c)
                {
                    if (sciNot >= 0)
                    {
                        throw new Exception("Unexpected scientific notation appearing twice in number literal");
                    }
                    else sciNot = 0;
                }
                else if (!char.IsDigit(c))
                {
                    break;
                }
                else
                {
                    int digit = c - '0';

                    if (sciNot >= 0)
                    {
                        sciNot *= 10;
                        sciNot += digit;
                    }
                    else if (fraction > 0)
                    {
                        value += Math.Pow(0.1, fraction) * digit;
                        fraction += 1;
                    }
                    else
                    {
                        value *= 10;
                        value += digit;
                    }
                }

                ++offset;
            }

            if (sciNot > 0)
            {
                value *= Math.Pow(10, sciNot);
            }
            else if (sciNot == 0)
            {
                throw new Exception("Dangling scientific notation in number literal");
            }

            return new Token { Kind = TokenKind.LiteralNumber, NumberValue = value, InputLength = offset - original };
        }

        private Token ParseOperator(string input, int offset)
        {
            var original = offset;
            offset += SkipWhitespace(input, offset);
            if (operatorDict.TryGetValue(input[offset], out Operator op))
            {
                return new Token { Kind = TokenKind.Operator, OperatorValue = op, InputLength = offset - original + 1 };
            }
            else
            {
                return null;
            }
        }

        public Command ParseCommand(string input, Context context)
        {
            CurrentSources.Clear();

            var first = NextToken(input);

            if (first.Kind != TokenKind.Keyword)
            {
                throw new Exception("Statement does not start with a keyword (unsupported as of now)");
            }

            return first.KeywordValue switch
            {
                Keyword.Select => ParseSelect(input, 0, context),
                _ => throw new NotImplementedException()
            };
        }

        private void ProcessExpressionNodeType(Expression expr, Context context, Expression.Node node)
        {
            Expression.ExpressionType nodeType;
            if (node.Kind == Expression.NodeKind.Reference)
            {
                SanitizeReference(node.ReferenceValue, context);

                var column = context.FindColumn(node.ReferenceValue);

                if (column != null)
                {
                    nodeType = Expression.ColumnTypeToExpressionType(column.Type);
                }
                else
                {
                    var variable = context.FindVariable(node.ReferenceValue.Column);
                    if (variable != null && string.IsNullOrEmpty(node.ReferenceValue.Table))
                    {
                        nodeType = Expression.ColumnTypeToExpressionType(variable.Type);
                    }
                    else
                    {
                        throw new Exception("Unresolved reference in expression");
                    }
                }

            } else if (node.Kind == Expression.NodeKind.Number)
            {
                nodeType = Expression.ExpressionType.Number;
            } else if (node.Kind == Expression.NodeKind.String)
            {
                nodeType = Expression.ExpressionType.String;
            } else
            {
                return;
            }


            if (expr.Type == Expression.ExpressionType.Unknown)
            {
                expr.Type = nodeType;
            }
            else if (expr.Type != nodeType)
            {
                throw new Exception("Incompatible value types in expression");
            }
        }

        private int ParseSubExpression(string input, int offset, Context context, ExpressionFlags flags, Expression expr, bool root, bool resolveReferences)
        {
            bool lastWasOperator = true;
            bool first = true;

            do
            {
                var token = NextToken(input, offset);

                if (offset >= input.Length || token == null || token.Kind == TokenKind.None)
                {
                    break;
                }

                offset += token.InputLength;

                if (token.Kind == TokenKind.Keyword)
                {
                    if (token.KeywordValue == Keyword.As)
                    {
                        if (!flags.IdentifierAllowed)
                        {
                            throw new Exception("Alias not allowed in this context");
                        }
                        else if (!root)
                        {
                            throw new Exception("Unexpected token AS in expression sub scope");
                        }
                        else if (expr.IsWildcard())
                        {
                            throw new Exception("Unexpected alias on wildcard");
                        }

                        var reference = ParseReference(input, offset, context, flags.WildcardAllowed, resolveReferences);
                        expr.Identifier = reference;

                        if (!string.IsNullOrEmpty(reference.Table))
                        {
                            throw new Exception("Invalid expression alias");
                        }

                        offset += reference.InputLength;

                        token = NextToken(input, offset);
                        if (token.Kind == TokenKind.Comma)
                        {
                            offset += token.InputLength;
                            break;
                        }
                        else if (token.Kind == TokenKind.Keyword)
                        {
                            // Continue to next iteration
                        }
                        else
                        {
                            throw new Exception("Unexpected token after expression alias");
                        }
                    }
                    else
                    {
                        if (root)
                        {
                            offset -= token.InputLength;
                            break;
                        }
                        else
                        {
                            throw new Exception("Unexpected keyword in expression sub scope");
                        }
                    }
                }
                else if (token.Kind == TokenKind.Comma)
                {
                    if (!root)
                    {
                        throw new Exception("Unexpected comma in middle of expression");
                    }
                    offset -= token.InputLength;
                    break;
                }
                else if (token.Kind == TokenKind.Operator)
                {
                    if (lastWasOperator)
                    {
                        if (first && ((root && token.OperatorValue == Operator.Asterisk) || token.OperatorValue == Operator.Plus ||
                            token.OperatorValue == Operator.Minus || token.OperatorValue == Operator.ParenthesisLeft || token.OperatorValue == Operator.ParenthesisRight))
                        {
                            if (token.OperatorValue == Operator.Asterisk && !flags.GeneralWildcardAllowed)
                            {
                                throw new Exception("General wildcard is not allowed in this context");
                            }
                            // else it's ok
                        }
                        else
                        {
                            throw new Exception("Two subsequent operators in expression");
                        }
                    }

                    lastWasOperator = true;

                    if (token.OperatorValue == Operator.Equal)
                    {
                        expr.IsBoolean = true;
                    }

                    expr.Nodes.Add(new Expression.Node() { Kind = Expression.NodeKind.Operator, OperatorValue = token.OperatorValue });

                    if (token.OperatorValue == Operator.ParenthesisLeft)
                    {
                        offset = ParseSubExpression(input, offset, context, flags, expr, false, resolveReferences);
                        lastWasOperator = false;
                    }
                    else if (token.OperatorValue == Operator.ParenthesisRight)
                    {
                        if (!root)
                        {
                            break;
                        }
                        else
                        {
                            throw new Exception("Missing left parenthesis");
                        }
                    }
                }
                else if (token.Kind == TokenKind.LiteralNumber || token.Kind == TokenKind.LiteralString || token.Kind == TokenKind.Symbol)
                {
                    if (!lastWasOperator)
                    {
                        throw new Exception("Two subsequent values in expression");
                    }
                    else if (expr.IsWildcard())
                    {
                        throw new Exception("Unexpected token after wildcard");
                    }

                    lastWasOperator = false;

                    if (token.Kind == TokenKind.Symbol)
                    {
                        offset -= token.InputLength;
                        // Only allow table wildcards (e.g. tablename.*) on root scope, when no other value or operator was provided
                        var symbol = ParseReference(input, offset, context, root && expr.Nodes.Count == 0 && flags.WildcardAllowed, resolveReferences);
                        var symNode = new Expression.Node() { Kind = Expression.NodeKind.Reference, ReferenceValue = symbol };

                        if (resolveReferences)
                        {
                            ProcessExpressionNodeType(expr, context, symNode);
                        } else
                        {
                            expr.Type = Expression.ExpressionType.Unknown;
                        }

                        expr.Nodes.Add(symNode);
                        offset += symbol.InputLength;
                    }
                    else if (token.Kind == TokenKind.LiteralNumber)
                    {
                        var num = token.NumberValue;
                        var numNode = new Expression.Node() { Kind = Expression.NodeKind.Number, NumberValue = num };

                        ProcessExpressionNodeType(expr, context, numNode);

                        expr.Nodes.Add(numNode);
                    }
                    else if (token.Kind == TokenKind.LiteralString)
                    {
                        var strNode = new Expression.Node() { Kind = Expression.NodeKind.String, StringValue = token.StringValue };

                        ProcessExpressionNodeType(expr, context, strNode);

                        expr.Nodes.Add(strNode);
                    }
                }
                else
                {
                    throw new Exception("Unexpected token in expression");
                }

                if (first)
                {
                    first = false;
                }
            } while (true);

            return offset;
        }

        public Expression ParseExpression(string input, int offset, Context context, ExpressionFlags flags, bool resolveReferences)
        {
            var expr = new Expression();
            int originalOffset = offset;

            offset = ParseSubExpression(input, offset, context, flags, expr, true, resolveReferences);

            if (expr.Type == Expression.ExpressionType.Unknown && !expr.IsWildcard() && resolveReferences)
            {
                throw new Exception("Fatal error, expression doesn't have a type but it's not a wildcard");
            }

            expr.FullText = input[originalOffset..offset];
            return expr;
        }

        public TableReference ParseTableReference(string input, int offset, Context context, bool resolve)
        {
            var reference = new TableReference();

            int originalOffset = offset;

            do
            {
                var token = NextToken(input, offset);
                offset += token.InputLength;
                if (token.Kind != TokenKind.Symbol)
                {
                    throw new Exception("Expected valid symbol for reference");
                }


                if (string.IsNullOrEmpty(reference.Table))
                {
                    reference.Table = token.StringValue;
                }
                else if (string.IsNullOrEmpty(reference.Schema))
                {
                    reference.Schema = reference.Table;
                    reference.Table = token.StringValue;
                    break;
                }

                token = NextToken(input, offset);
                if (token != null && token.Kind == TokenKind.Dot)
                {
                    offset += token.InputLength;
                }
                else
                {
                    break;
                }
            } while (true);

            reference.InputLength = offset - originalOffset;

            if (resolve && !context.IsReferenceValid(reference))
            {
                throw new Exception("Unresolved reference");
            }

            return reference;
        }

        private void SanitizeReference(ColumnReference reference, Context context)
        {
            if (string.IsNullOrEmpty(reference.Table) && !context.IsReferenceValid(reference))
            {
                var results = CurrentSources.Select(s => context.FindTable(s)).Where(t => context.FindColumn(new ColumnReference() { Table = t.Name, Column = reference.Column }) != null);

                if (results.Count() > 1)
                {
                    throw new Exception($"Ambiguous column name \"{reference.Column}\"");
                }
                else if (!results.Any())
                {
                    throw new Exception("Unresolved reference");
                }

                reference.Table = results.First().Name;
            }

            if (!context.IsReferenceValid(reference))
            {
                throw new Exception("Unresolved column reference");
            }
        }

        public ColumnReference ParseReference(string input, int offset, Context context, bool allowWildcard, bool resolveReferences)
        {
            int originalOffset = offset;
            var table = ParseTableReference(input, offset, context, false);
            offset += table.InputLength;
            ColumnReference reference;

            var token = NextToken(input, offset);
            if (token.Kind != TokenKind.Dot)
            {
                reference = new() { Column = table.Table, Table = table.Schema };
            }
            else
            {
                offset += token.InputLength;
                token = NextToken(input, offset);
                offset += token.InputLength;

                if (token.Kind == TokenKind.Operator && token.OperatorValue == Operator.Asterisk)
                {
                    if (!allowWildcard)
                    {
                        throw new Exception("Unexpected wildcard in context where it's not permitted");
                    }
                    else if (table.Table == "*")
                    {
                        throw new Exception("Malformed column reference: Expected table name but a wildcard was provided");
                    }

                    reference = new() { Column = "*", Schema = table.Schema, Table = table.Table };
                }
                else if (token.Kind != TokenKind.Symbol)
                {
                    throw new Exception("Unexpected token after . in reference");
                }
                else
                {
                    reference = new() { Column = token.StringValue, Schema = table.Schema, Table = table.Table };
                }
            }

            if (resolveReferences)
            {
                SanitizeReference(reference, context);
            }

            reference.InputLength = offset - originalOffset;

            return reference;
        }

        private void ResolveUnresolvedReferences(Expression expr, Context context)
        {
            foreach (var node in expr.Nodes)
            {
                ProcessExpressionNodeType(expr, context, node);
            }
        }

        public SelectCommand ParseSelect(string input, int offset, Context context)
        {
            int originalOffset = offset;
            var command = new SelectCommand();

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

                if (command.OutputColumns.Count > 0 && expr.IsWildcard())
                {
                    throw new Exception("Unexpected token *");
                }

                command.OutputColumns.Add(expr);
                offset += expr.FullText.Length;

                // Disallow general wildflags after first column
                exprFlags.GeneralWildcardAllowed = false;
            } while (true);

            var source = ParseTableReference(input, offset, context, true);
            CurrentSources.Add(source);

            foreach (var expr in command.OutputColumns.Where(e => e.Type == Expression.ExpressionType.Unknown))
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
