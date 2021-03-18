using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace wooby
{
    public static class Parser
    {
        private enum TokenKind
        {
            LiteralNumber,
            LiteralString,
            Symbol,
            Keyword,
            Operator,
            Comma,
            SemiColon,
            None
        }

        private static readonly Dictionary<string, Keyword> keywordDict = new()
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
            { "BY", Keyword.By }
        };

        private static readonly Dictionary<char, Operator> operatorDict = new()
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

        private class Token
        {
            public TokenKind Kind;
            public string StringValue;
            public double NumberValue;
            public Keyword KeywordValue;
            public Operator OperatorValue;
            public int InputLength;
        }

        private static TokenKind PeekToken(string input, int offset)
        {
            var first = input[offset];

            if(char.IsDigit(first))
            {
                return TokenKind.LiteralNumber;
            } else if(first == '\"' || first == '_' || char.IsLetter(first))
            {
                return TokenKind.Symbol;
            } else if(operatorDict.ContainsKey(first))
            {
                return TokenKind.Operator;
            } else return TokenKind.None;
        }

        private static int SkipWhitespace(string input, int offset)
        {
            int original = offset;

            foreach(var c in input[offset..])
            {
                if(!char.IsWhiteSpace(c))
                {
                    break;
                }
            }

            return offset - original;
        }

        private static Token NextToken(string input, int offset = 0)
        {
            if(offset >= input.Length)
                return null;

            var skipped = SkipWhitespace(input, offset);
            offset += skipped;

            var result = input[offset] switch
            {
                ',' => new Token { Kind = TokenKind.Comma, InputLength = 1 },
                ';' => new Token { Kind = TokenKind.SemiColon, InputLength = 1 },
                '\'' => ParseString(input, offset),
                _ => PeekToken(input, offset) switch
                {
                    TokenKind.Symbol => ParseSymbol(input, offset),
                    TokenKind.LiteralNumber => ParseNumber(input, offset),
                    TokenKind.Operator => ParseOperator(input, offset),
                    _ => null
                }
            };

            result.InputLength += skipped;
            return result;
        }

        private static Token ParseSymbol(string input, int offset)
        {
            int originalOffset = offset;

            foreach(var c in input[offset..])
            {
                if((c == '\"' && originalOffset != offset) || char.IsWhiteSpace(c) || !(char.IsDigit(c) || char.IsLetter(c)) || char.IsControl(c))
                {
                    break;
                }

                ++offset;
            }

            string symbol = input[originalOffset..offset];

            if(keywordDict.TryGetValue(symbol.ToUpper(), out Keyword keyword))
            {
                return new Token { Kind = TokenKind.Keyword, KeywordValue = keyword, InputLength = offset - originalOffset };
            } else
            {
                return new Token { Kind = TokenKind.Symbol, StringValue = symbol, InputLength = offset - originalOffset };
            }
        }

        private static Token ParseString(string input, int offset)
        {
            int original = offset;
            var lastWasEscape = false;

            foreach(var c in input[offset..])
            {
                if(lastWasEscape)
                {
                    lastWasEscape = false;
                } else if(c == '\\')
                {
                    lastWasEscape = true;
                } else if(c == '\'')
                {
                    break;
                }
            }

            return new Token { Kind = TokenKind.LiteralString, StringValue = input[original..offset], InputLength = offset - original };
        }

        private static Token ParseNumber(string input, int offset)
        {
            double value = 0d;
            int fraction = 0, sciNot = -1;

            int original = offset;

            foreach(var c in input[offset..])
            {
                if(c == '.')
                {
                    if(fraction > 0 || sciNot >= 0)
                    {
                        throw new Exception("Unexpected '.' in number literal");
                    } else fraction = 1;
                } else if ('e' == c || 'E' == c)
                {
                    if(sciNot >= 0)
                    {
                        throw new Exception("Unexpected scientific notation appearing twice in number literal");
                    } else sciNot = 0;
                } else if(!char.IsDigit(c))
                {
                    break;
                }

                int digit = c - '0';

                if(sciNot >= 0)
                {
                    sciNot *= 10;
                    sciNot += digit;
                } else if(fraction > 0)
                {
                    value += Math.Pow(0.1, fraction) * digit;
                    fraction += 1;
                } else
                {
                    value *= 10;
                    value += digit;
                }

                ++offset;
            }

            if(sciNot > 0)
            {
                value *= Math.Pow(10, sciNot);
            } else if(sciNot == 0)
            {
                throw new Exception("Dangling scientific notation in number literal");
            }

            return new Token { Kind = TokenKind.LiteralNumber, NumberValue = value, InputLength = offset - original };
        }

        private static Token ParseOperator(string input, int offset)
        {
            if(operatorDict.TryGetValue(input[offset], out Operator op))
            {
                return new Token { Kind = TokenKind.Operator, OperatorValue = op, InputLength = 1 };
            } else
            {
                return null;
            }
        }

        public static Command ParseCommand(string input, Context context)
        {
            var first = NextToken(input);

            if(first.Kind != TokenKind.Keyword)
            {
                throw new Exception("Statement does not start with a keyword (unsupported as of now)");
            }

            return first.KeywordValue switch
            {
                Keyword.Select => ParseSelect(input, first.InputLength, context),
                _ => throw new NotImplementedException()
            };
        }

        public static Expression ParseExpression(string input, int offset, Context context)
        {
            throw new NotImplementedException();
        }

        public static ColumnReference ParseReference(string input, int offset, Context context)
        {
            throw new NotImplementedException();
        }

        public static SelectCommand ParseSelect(string input, int offset, Context context)
        {
            var command = new SelectCommand();

            Token next;

            do
            {
                next = NextToken(input, offset);
                if(next.Kind == TokenKind.Keyword && next.KeywordValue == Keyword.From)
                {
                    offset += next.InputLength;
                    break;
                } else if(next.Kind == TokenKind.None)
                {
                    throw new Exception("Unexpected end of input");
                } else if(next.Kind == TokenKind.Comma)
                {
                    offset += next.InputLength;
                }

                var expr = ParseExpression(input, offset, context);
                command.OutputColumns.Add(expr);
                offset += expr.FullText.Length;
            } while (true);

            var source = ParseReference(input, offset, context);
            if(source.Column.Length > 0)
            {
                throw new Exception("Expected table name after FROM keyword");
            }

            command.MainSource = source;
            offset += source.InputLength;

            do
            {
                next = NextToken(input, offset);
                if(next.Kind == TokenKind.Keyword)
                {
                    if(next.KeywordValue == Keyword.Where)
                    {
                        if(command.FilterConditions != null)
                        {
                            throw new Exception("Unexpected WHERE when filter has already been set");
                        }

                        command.FilterConditions = ParseExpression(input, offset, context);
                        offset += command.FilterConditions.FullText.Length;
                    } else if(next.KeywordValue == Keyword.Order)
                    {
                        next = NextToken(input, offset);
                        if(next.Kind != TokenKind.Keyword || next.KeywordValue != Keyword.By)
                        {
                            throw new Exception("Unexpected token after ORDER");
                        }

                        offset += next.InputLength;

                        var orderExpr = ParseExpression(input, offset, context);
                        command.OutputOrder = new Ordering { OrderExpression = orderExpr };
                        offset += orderExpr.FullText.Length;

                        next = NextToken(input, offset);
                        if(next.Kind == TokenKind.Keyword && (next.KeywordValue == Keyword.Asc || next.KeywordValue == Keyword.Desc))
                        {
                            command.OutputOrder.Kind = next.KeywordValue == Keyword.Asc ? OrderingKind.Ascending : OrderingKind.Descending;
                            offset += next.InputLength;
                        }
                    }
                } else
                {
                    throw new Exception($"Unexpected token in query at offset {offset}");
                }
            } while (next.Kind == TokenKind.None || next.Kind == TokenKind.SemiColon);

            return command;
        }
    }
}
