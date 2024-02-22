using System;
using System.Collections.Generic;
using System.Linq;
using wooby.Database;
using wooby.Database.Defaults;
using wooby.Error;

namespace wooby.Parsing;

public partial class Parser
{
    private readonly Dictionary<string, Keyword> _keywordDict = new()
    {
        {"SELECT", Keyword.Select},
        {"FROM", Keyword.From},
        {"WHERE", Keyword.Where},
        {"NOT", Keyword.Not},
        {"NULL", Keyword.Null},
        {"IS", Keyword.Is},
        {"TRUE", Keyword.True},
        {"FALSE", Keyword.False},
        {"AND", Keyword.And},
        {"OR", Keyword.Or},
        {"ASC", Keyword.Asc},
        {"DESC", Keyword.Desc},
        {"ORDER", Keyword.Order},
        {"GROUP", Keyword.Group},
        {"BY", Keyword.By},
        {"AS", Keyword.As},
        {"CREATE", Keyword.Create},
        {"INSERT", Keyword.Insert},
        {"UPDATE", Keyword.Update},
        {"DELETE", Keyword.Delete},
        {"ALTER", Keyword.Alter},
        {"SET", Keyword.Set},
        {"INTO", Keyword.Into},
        {"VALUES", Keyword.Values},
        {"TABLE", Keyword.Table},
        {"COLUMN", Keyword.Column},
        {"ADD", Keyword.Add},
        {"CONSTRAINT", Keyword.Constraint},
        {"DISTINCT", Keyword.Distinct},
        {"LEFT", Keyword.Left},
        {"RIGHT", Keyword.Right},
        {"INNER", Keyword.Inner},
        {"JOIN", Keyword.Join},
        {"ON", Keyword.On},
        {"HAVING", Keyword.Having},
    };

    private readonly Dictionary<string, Operator> _operatorDict = new()
    {
        {"+", Operator.Plus},
        {"-", Operator.Minus},
        {"/", Operator.ForwardSlash},
        {"*", Operator.Asterisk},
        {"(", Operator.ParenthesisLeft},
        {")", Operator.ParenthesisRight},
        {"^", Operator.Power},
        {"<", Operator.LessThan},
        {">", Operator.MoreThan},
        {"=", Operator.Equal},
        {"<>", Operator.NotEqual},
        {"!=", Operator.NotEqual},
        {"<=", Operator.LessEqual},
        {">=", Operator.MoreEqual},
        {"%", Operator.Remainder},
        {"||", Operator.Plus}
    };

    private readonly Operator[] _booleanOperators =
    {
        Operator.Equal,
        Operator.LessThan,
        Operator.MoreThan,
        Operator.NotEqual,
        Operator.LessEqual,
        Operator.MoreEqual,
    };

    private TokenKind PeekToken(string input, int offset)
    {
        var first = input[offset];

        if (char.IsDigit(first))
        {
            return TokenKind.LiteralNumber;
        }

        if (first is '\"' or '_' || char.IsLetter(first))
        {
            return TokenKind.Symbol;
        }

        if (_operatorDict.ContainsKey(input.Substring(offset, 1)) ||
            _operatorDict.ContainsKey(input.Substring(offset, 2)))
        {
            return TokenKind.Operator;
        }

        return TokenKind.None;
    }

    private static int SkipWhitespace(string input, int offset)
    {
        var original = offset;

        foreach (var _ in input[offset..].TakeWhile(char.IsWhiteSpace))
        {
            ++offset;
        }

        return offset - original;
    }

    private static bool IsKeywordOperator(Keyword kw, out Operator op)
    {
        const Operator defaultValue = Operator.ParenthesisLeft;

        op = kw switch
        {
            Keyword.And => Operator.And,
            Keyword.Or => Operator.Or,
            _ => defaultValue
        };

        return op != defaultValue;
    }

    private Token NextToken(string input, int offset = 0)
    {
        if (offset >= input.Length)
        {
            return new Token {Kind = TokenKind.None};
        }

        var originalOffset = offset;
        var skipped = SkipWhitespace(input, offset);

        var result = input[offset + skipped] switch
        {
            ',' => new Token {Kind = TokenKind.Comma, InputLength = 1, FullText = ","},
            ';' => new Token {Kind = TokenKind.SemiColon, InputLength = 1, FullText = ";"},
            '.' => new Token {Kind = TokenKind.Dot, InputLength = 1, FullText = "."},
            '\'' => ParseString(input, offset),
            _ => PeekToken(input, offset + skipped) switch
            {
                TokenKind.Symbol => ParseSymbol(input, offset),
                TokenKind.LiteralNumber => ParseNumber(input, offset),
                TokenKind.Operator => ParseOperator(input, offset) ?? new Token {Kind = TokenKind.None},
                _ => new Token {Kind = TokenKind.None}
            }
        };

        if (result.Kind == TokenKind.Keyword)
        {
            if (IsKeywordOperator(result.KeywordValue, out var op))
            {
                result.Kind = TokenKind.Operator;
                result.OperatorValue = op;
            }
        }

        result.FullText = input.Substring(originalOffset, offset - originalOffset);

        return result;
    }

    private static void AssertTokenIsKeyword(Token token, Keyword keyword, string message)
    {
        if (!token.IsKeyword() || token.KeywordValue != keyword)
        {
            throw new Exception(message);
        }
    }

    private static void AssertTokenIsOperator(Token token, Operator op, string message)
    {
        if (!token.IsOperator() || token.OperatorValue != op)
        {
            throw new Exception(message);
        }
    }

    private static void AssertTokenIsSymbol(Token token, string message)
    {
        if (token.Kind != TokenKind.Symbol)
        {
            throw new Exception(message);
        }
    }

    private void SkipNextToken(string input, ref int offset)
    {
        var token = NextToken(input, offset);
        offset += token.InputLength;
    }

    private Token ParseSymbol(string input, int offset)
    {
        // FIXME: Should be able to parse symbols enclosed in double quotes ""
        var originalOffset = offset;

        offset += SkipWhitespace(input, offset);
        var start = offset;

        foreach (var _ in input[offset..].TakeWhile(c =>
                     (c != '\"' || originalOffset == offset) && !char.IsWhiteSpace(c) &&
                     (char.IsDigit(c) || char.IsLetter(c) || c == '_') && !char.IsControl(c)))
        {
            ++offset;
        }

        var symbol = input[start..offset];

        var fullText = input[originalOffset..offset];
        return _keywordDict.TryGetValue(symbol.ToUpper(), out var keyword)
            ? new Token {Kind = TokenKind.Keyword, KeywordValue = keyword, InputLength = offset - originalOffset, FullText = fullText}
            : new Token {Kind = TokenKind.Symbol, StringValue = symbol, InputLength = offset - originalOffset, FullText = fullText};
    }

    private static Token ParseString(string input, int offset)
    {
        var original = offset;

        offset += SkipWhitespace(input, offset) + 1;
        var start = offset;

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

        return new Token
        {
            Kind = TokenKind.LiteralString,
            StringValue = input[start..(offset - 1)],
            FullText = input[original..offset],
            InputLength = offset - original
        };
    }

    private static Token ParseNumber(string input, int offset)
    {
        var value = 0d;
        int fraction = 0, sciNot = -1;

        var original = offset;

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
                var digit = c - '0';

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

        return new Token
        {
            Kind = TokenKind.LiteralNumber, NumberValue = value, InputLength = offset - original,
            FullText = input[original..offset],
        };
    }

    private Token? ParseOperator(string input, int offset)
    {
        var original = offset;
        offset += SkipWhitespace(input, offset);
        if (input.Length - offset > 2 && _operatorDict.TryGetValue(input.Substring(offset, 2), out var op))
        {
            return new Token
            {
                Kind = TokenKind.Operator, OperatorValue = op,
                FullText = input[offset].ToString(),
                InputLength = offset - original + 2
            };
        }

        if (_operatorDict.TryGetValue(input.Substring(offset, 1), out var o))
        {
            return new Token
            {
                Kind = TokenKind.Operator,
                OperatorValue = o,
                FullText = input[offset].ToString(),
                InputLength = offset - original + 1
            };
        }

        return null;
    }

    public Statement ParseStatement(string input, Context context)
    {
        var first = NextToken(input);

        if (first.Kind != TokenKind.Keyword)
        {
            throw new Exception("Statement does not start with a keyword (unsupported as of now)");
        }

        return first.KeywordValue switch
        {
            Keyword.Select => ParseSelect(input, 0, context, new StatementFlags(), null),
            Keyword.Create => ParseCreate(input, 0),
            Keyword.Insert => ParseInsert(input, 0, context),
            Keyword.Update => ParseUpdate(input, 0, context),
            Keyword.Delete => ParseDelete(input, 0, context),
            _ => throw new WoobyParserException("Invalid starting keyword", 0, first)
        };
    }

    private static void ProcessExpressionNodeType(Expression expr, Context context, Statement statement, Expression.Node node)
    {
        Expression.ExpressionType nodeType;
        if (node.Kind == Expression.NodeKind.Reference)
        {
            if (node.ReferenceValue == null)
                throw new WoobyException("Internal error: Reference is null");
            SanitizeReference(node.ReferenceValue, context, statement);
            
            if (node.ReferenceValue.Column == "*")
            {
                //nodeType = Expression.ExpressionType.Unknown;
            }
            else if (node.ReferenceValue.Type == Expression.ExpressionType.Unknown)
            {
                throw new Exception($"Undefined reference to {node.ReferenceValue.Join()}");
            }

            nodeType = node.ReferenceValue.Type;
        }
        else if (node.Kind == Expression.NodeKind.Function)
        {
            if (node.FunctionCall == null)
                throw new WoobyParserException("Internal error: FunctionCall is null", 0);
            nodeType = Expression.ColumnTypeToExpressionType(node.FunctionCall.CalledVariant.ResultType);

            foreach (var arg in node.FunctionCall.Arguments)
            {
                ResolveUnresolvedReferences(arg, context, statement);
            }

            ValidateFunctionCall(node.FunctionCall);
            if (node.FunctionCall.CalledVariant.IsAggregate)
            {
                expr.HasAggregateFunction = true;
            }
        }
        else if (node.Kind == Expression.NodeKind.Number)
        {
            nodeType = Expression.ExpressionType.Number;
        }
        else if (node.Kind == Expression.NodeKind.String)
        {
            nodeType = Expression.ExpressionType.String;
        }
        else if (node.Kind == Expression.NodeKind.Null)
        {
            nodeType = Expression.ExpressionType.Null;
        }
        else if (node.Kind == Expression.NodeKind.SubSelect)
        {
            if (node.SubSelect == null)
                throw new WoobyException("Internal error: SubSelect is null");
            ResolveSelectReferences(node.SubSelect, context);
            nodeType = node.SubSelect.OutputColumns[0].Type;
        }
        else
        {
            return;
        }

        if (expr.Type == Expression.ExpressionType.Unknown)
        {
            expr.Type = nodeType;
        }
        else if (expr.Type != nodeType && nodeType != Expression.ExpressionType.Null)
        {
            throw new Exception("Incompatible value types in expression");
        }
    }

    private List<Expression> ParseFunctionArguments(string input, ref int ogOffset, Context context,
        Statement statement, bool resolveReferences)
    {
        var exprList = new List<Expression>();
        var flags = new ExpressionFlags
            {GeneralWildcardAllowed = false, IdentifierAllowed = false, WildcardAllowed = false};

        var offset = ogOffset;

        while (true)
        {
            var next = NextToken(input, offset);
            if (next.Kind == TokenKind.Comma)
            {
                if (exprList.Count > 0)
                {
                    offset += next.InputLength;
                }
                else
                {
                    throw new Exception("Expected value in function argument list, found comma");
                }
            }
            else if (next is { Kind: TokenKind.Operator, OperatorValue: Operator.ParenthesisRight })
            {
                offset += next.InputLength;
                break;
            }
            else if (exprList.Count > 0)
            {
                throw new Exception("Unexpected token inside function argument list");
            }

            var expr = ParseExpression(input, offset, context, statement, flags, resolveReferences, true);
            exprList.Add(expr);

            offset += expr.FullText.Length;
        }

        ogOffset = offset;

        return exprList;
    }

    private static void ValidateFunctionCall(FunctionCall func)
    {
        // Retry to find the correct variant for the call
        func.CalledVariant = TryFindSuitableVariantForCall(func.Meta, func.Arguments);
        if (func.CalledVariant == null)
        {
            throw new Exception($"No suitable variant for function call of {func.Meta.Name} (during validation)");
        }

        for (var i = 0; i < func.CalledVariant.Parameters.Count; ++i)
        {
            var expected = Expression.ColumnTypeToExpressionType(func.CalledVariant.Parameters[i]);
            var argExpression = func.Arguments[i];

            if ((expected != Expression.ExpressionType.Null && expected != argExpression.Type) ||
                argExpression.IsBoolean)
            {
                throw new Exception($"Argument #{i} does not match the function definition");
            }
        }
    }

    private static FunctionAccepts TryFindSuitableVariantForCall(Function meta, List<Expression> parameters)
    {
        var candidates = meta.Variations.Where(v => v.Parameters.Count == parameters.Count);
        var provided = parameters.Select(expr => expr.Type).ToList();
        if (provided.Any(p => p == Expression.ExpressionType.Unknown))
        {
            // Assume the first is correct and come back later to find the correct variant. Rarely it'll vary in the important
            // bits such as whether it's aggregate or not, with the same number of arguments
            return candidates.First();
        }

        foreach (var candidate in candidates)
        {
            if (provided.SequenceEqual(candidate.Parameters.Select(Expression.ColumnTypeToExpressionType)))
            {
                return candidate;
            }
        }

        throw new Exception($"No suitable function call for {meta.Name} matches the given parameters");
    }

    private int ParseSubExpression(string input, int offset, Context context, Statement statement,
        ExpressionFlags flags, Expression expr, bool root, bool resolveReferences, bool insideFunction)
    {
        var lastWasOperator = true;
        var lastWasReference = false;
        var first = true;

        var referenceFlags = new ReferenceFlags {ResolveReferences = resolveReferences};

        do
        {
            var token = NextToken(input, offset);

            if (offset >= input.Length || token.Kind is TokenKind.None)
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
                    if (!root)
                    {
                        throw new Exception("Unexpected token AS in expression sub scope");
                    }
                    if (expr.IsWildcard())
                    {
                        throw new Exception("Unexpected alias on wildcard");
                    }

                    token = NextToken(input, offset);
                    if (token.Kind != TokenKind.Symbol)
                    {
                        throw new Exception("Expected symbol after AS keyword");
                    }

                    expr.Identifier = token.StringValue;
                    offset += token.InputLength;

                    token = NextToken(input, offset);
                    if (token.Kind == TokenKind.Comma)
                    {
                        break;
                    }
                    if (token.Kind == TokenKind.Keyword)
                    {
                        // Continue to next iteration
                    }
                    else
                    {
                        throw new Exception("Unexpected token after expression alias");
                    }
                }
                else if (token.KeywordValue == Keyword.Null)
                {
                    if (!lastWasOperator)
                    {
                        throw new Exception("Two consecutive values in expression");
                    }

                    lastWasOperator = false;

                    var node = new Expression.Node {Kind = Expression.NodeKind.Null};
                    expr.Nodes.Add(node);
                    ProcessExpressionNodeType(expr, context, statement, node);
                }
                else if (token.KeywordValue == Keyword.Select)
                {
                    if (!flags.SingleValueSubSelectAllowed)
                    {
                        throw new Exception("Sub select is not allowed in this context");
                    }
                    if (!first)
                    {
                        throw new Exception("Unexpected SELECT keyword");
                    }

                    // sub select scope
                    expr.Nodes.RemoveAt(expr.Nodes.Count - 1);

                    offset -= token.InputLength;
                    var result = ParseSelect(input, offset, context, statement.UsedFlags, statement);
                    offset += result.OriginalText.Length;
                    var node = new Expression.Node {Kind = Expression.NodeKind.SubSelect, SubSelect = result};
                    expr.Nodes.Add(node);

                    // This sub scope is only for the sub select
                    break;
                }
                else
                {
                    if (!root) throw new Exception("Unexpected keyword in expression sub scope");
                    offset -= token.InputLength;
                    break;
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
                    if (first && ((root && token.OperatorValue == Operator.Asterisk) ||
                                  token.OperatorValue == Operator.Plus ||
                                  token.OperatorValue == Operator.Minus ||
                                  token.OperatorValue == Operator.ParenthesisLeft ||
                                  token.OperatorValue == Operator.ParenthesisRight))
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

                if (_booleanOperators.Contains(token.OperatorValue))
                {
                    expr.IsBoolean = true;
                }

                expr.Nodes.Add(new Expression.Node {Kind = Expression.NodeKind.Operator, OperatorValue = token.OperatorValue});

                if (token.OperatorValue == Operator.ParenthesisLeft)
                {
                    if (lastWasReference)
                    {
                        // Function call
                        expr.Nodes.RemoveAt(expr.Nodes.Count - 1);

                        var lastReference = expr.Nodes.Last().ReferenceValue;
                        if (lastReference == null)
                        {
                            throw new WoobyParserException("Internal error: Reference is null", offset, token);
                        }
                        if (!string.IsNullOrEmpty(lastReference.Table))
                        {
                            throw new Exception($"Undefined reference to function call \"{lastReference.Join()}\"");
                        }

                        var funcMeta = context.FindFunction(lastReference.Column);
                        if (funcMeta == null)
                        {
                            throw new Exception("Undefined reference to function");
                        }

                        var arguments = ParseFunctionArguments(input, ref offset, context, statement,
                            resolveReferences);
                        var variant = TryFindSuitableVariantForCall(funcMeta, arguments);
                        var func = new FunctionCall(funcMeta, variant, arguments);

                        if (func.CalledVariant.IsAggregate && !flags.AllowAggregateFunctions)
                        {
                            throw new Exception("Aggregate functions not allowed in this context");
                        }

                        expr.Nodes[^1] = new Expression.Node {Kind = Expression.NodeKind.Function, FunctionCall = func};
                        lastWasOperator = false;
                    }
                    else
                    {
                        offset = ParseSubExpression(input, offset, context, statement, flags, expr, false,
                            resolveReferences, insideFunction);
                        lastWasOperator = false;
                    }
                }
                else if (token.OperatorValue == Operator.ParenthesisRight)
                {
                    if (root && !insideFunction && statement.UsedFlags.StopOnUnmatchedParenthesis)
                        throw new Exception("Missing left parenthesis");
                    if (insideFunction || !statement.UsedFlags.StopOnUnmatchedParenthesis)
                    {
                        offset -= token.InputLength;
                    }
                    else if (expr.Nodes[^2].Kind == Expression.NodeKind.Function)
                    {
                        expr.Nodes.RemoveAt(expr.Nodes.Count - 1);
                    }

                    break;
                }

                lastWasReference = false;
            }
            else if (token.Kind is TokenKind.LiteralNumber or TokenKind.LiteralString or TokenKind.Symbol)
            {
                lastWasReference = false;

                if (!lastWasOperator)
                {
                    throw new Exception("Two subsequent values in expression");
                }

                if (expr.IsWildcard())
                {
                    throw new Exception("Unexpected token after wildcard");
                }

                lastWasOperator = false;

                if (token.Kind == TokenKind.Symbol)
                {
                    var peekNext = NextToken(input, offset);
                    offset -= token.InputLength;
                    // Only allow table wildcards (e.g. table_name.*) on root scope, when no other value or operator was provided
                    referenceFlags.WildcardAllowed = root && expr.Nodes.Count == 0 && flags.WildcardAllowed;
                    var symbol = ParseReference(input, offset, context, statement, referenceFlags);
                    var symNode = new Expression.Node {Kind = Expression.NodeKind.Reference, ReferenceValue = symbol};

                    // If it's a function, leave it so that the code above can deal with it
                    if (resolveReferences && peekNext is not { Kind: TokenKind.Operator, OperatorValue: Operator.ParenthesisLeft })
                    {
                        ProcessExpressionNodeType(expr, context, statement, symNode);
                    }
                    else
                    {
                        expr.Type = Expression.ExpressionType.Unknown;
                    }

                    expr.Nodes.Add(symNode);
                    offset += symbol.InputLength;

                    lastWasReference = true;
                }
                else if (token.Kind == TokenKind.LiteralNumber)
                {
                    var num = token.NumberValue;
                    var numNode = new Expression.Node {Kind = Expression.NodeKind.Number, NumberValue = num};

                    ProcessExpressionNodeType(expr, context, statement, numNode);

                    expr.Nodes.Add(numNode);
                }
                else if (token.Kind == TokenKind.LiteralString)
                {
                    var strNode = new Expression.Node {Kind = Expression.NodeKind.String, StringValue = token.StringValue};

                    ProcessExpressionNodeType(expr, context, statement, strNode);

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

    private Expression ParseExpression(string input, int offset, Context context, Statement statement,
        ExpressionFlags flags, bool resolveReferences, bool insideFunction)
    {
        var expr = new Expression();
        var originalOffset = offset;

        offset = ParseSubExpression(input, offset, context, statement, flags, expr, true, resolveReferences,
            insideFunction);
        expr.FullText = input[originalOffset..offset];

        if (expr.Identifier != null) return expr;
        
        if (expr.IsOnlyFunctionCall())
        {
            expr.Identifier = expr.FullText.Trim();
        }
        else if (expr.IsOnlyReference())
        {
            expr.Identifier = expr.Nodes[0].ReferenceValue?.Join();
        }

        return expr;
    }

    private static void SanitizeReference(ColumnReference reference, Context context, Statement statement)
    {
        if (string.IsNullOrEmpty(reference.Column))
        {
            if (context.FindTable(reference) == null)
            {
                throw new Exception("Undefined reference to table");
            }
        }
        else
        {
            ColumnReference? r;
            if (string.IsNullOrEmpty(reference.Table))
            {
                var function = context.FindFunction(reference.Column);
                if (function != null) return;

                r = statement.TryFindReferenceRecursive(context, reference, 0);

                if (r == null)
                {
                    throw new Exception("Unresolved reference to column");
                }
                reference.Table = r.Table;
            }
            else
            {
                r = statement.TryFindReferenceRecursive(context, reference, 0);
                if (r == null)
                {
                    throw new Exception("Unresolved reference to column");
                }
            }
            
            // TODO: Check for name ambiguity

            reference.Type = r.Type;
        }
    }

    private ColumnReference ParseReference(string input, int offset, Context context, Statement statement,
        ReferenceFlags flags)
    {
        var reference = new ColumnReference();

        var originalOffset = offset;

        do
        {
            if (offset >= input.Length || (char.IsWhiteSpace(input[offset]) && offset > originalOffset))
            {
                break;
            }

            var token = NextToken(input, offset);
            offset += token.InputLength;
            if (token.Kind == TokenKind.None)
            {
                break;
            }
            if (token.Kind != TokenKind.Symbol &&
                     (token.Kind != TokenKind.Operator || token.OperatorValue != Operator.Asterisk))
            {
                throw new Exception("Expected valid symbol for reference");
            }

            if (string.IsNullOrEmpty(reference.Column))
            {
                reference.Column = token.StringValue;
            }
            else if (string.IsNullOrEmpty(reference.Table))
            {
                reference.Table = reference.Column;

                if (token is { Kind: TokenKind.Operator, OperatorValue: Operator.Asterisk })
                {
                    if (!flags.WildcardAllowed)
                    {
                        throw new Exception("Unexpected wildcard in context where it's not permitted");
                    }

                    reference.Column = "*";
                }
                else if (token.Kind != TokenKind.Symbol)
                {
                    throw new Exception("Unexpected token after . in reference");
                }
                else
                {
                    reference.Column = token.StringValue;
                }

                break;
            }

            token = NextToken(input, offset);
            if (token.Kind == TokenKind.Dot)
            {
                offset += token.InputLength;
            }
            else if (token is { Kind: TokenKind.Keyword, KeywordValue: Keyword.As } ||
                     token.Kind == TokenKind.Symbol)
            {
                if (token.Kind == TokenKind.Keyword)
                {
                    offset += token.InputLength;
                    if (!flags.AliasAllowed)
                    {
                        throw new Exception("Illegal alias in reference");
                    }

                    token = NextToken(input, offset);
                    if (token.Kind != TokenKind.Symbol)
                    {
                        throw new Exception("Expected symbol after AS keyword");
                    }
                }

                reference.Identifier = token.StringValue;
                offset += token.InputLength;
            }
            else
            {
                break;
            }
        } while (true);

        reference.InputLength = offset - originalOffset;

        if (string.IsNullOrEmpty(reference.Table) && flags.TableOnly)
        {
            reference.Table = reference.Column;
            reference.Column = "";

            if (string.IsNullOrEmpty(reference.Identifier))
            {
                reference.Identifier = reference.Table;
            }
        }

        if (flags.TableOnly && !string.IsNullOrEmpty(reference.Column))
        {
            throw new Exception("Expected table name");
        }

        if (flags.ResolveReferences)
        {
            SanitizeReference(reference, context, statement);
        }

        return reference;
    }

    private static void ResolveUnresolvedReferences(Expression expr, Context context, Statement statement)
    {
        foreach (var node in expr.Nodes)
        {
            ProcessExpressionNodeType(expr, context, statement, node);
        }
    }

    private ColumnType ParseColumnType(string input, int offset, out int deltaOffset)
    {
        var token = NextToken(input, offset);
        deltaOffset = token.InputLength;

        if (token.Kind != TokenKind.Symbol)
        {
            return ColumnType.Null;
        }

        return token.StringValue.ToUpper() switch
        {
            "TEXT" => ColumnType.String,
            "INT" or "INTEGER" or "NUMBER" => ColumnType.Number,
            "DATE" or "DATETIME" => ColumnType.Date,
            "BOOL" or "BOOLEAN" => ColumnType.Boolean,
            _ => ColumnType.Null
        };
    }

    private TableSource ParseTableSource(string input, int offset, Context context, Statement statement)
    {
        var next = NextToken(input, offset);
        if (next.Kind == TokenKind.Symbol)
        {
            return new TableSource
            {
                Kind = TableSource.SourceKind.Reference,
                Reference = ParseReference(input, offset, context, statement, new ReferenceFlags {TableOnly = true})
            };
        }
        if (next is { Kind: TokenKind.Operator, OperatorValue: Operator.ParenthesisLeft })
        {
            var flags = statement.UsedFlags;
            flags.ForceOutputIdentifiers = true;
            flags.SkipFirstParenthesis = true;
            flags.StopOnUnmatchedParenthesis = true;

            var source = new TableSource
            {
                Kind = TableSource.SourceKind.SubSelect,
                SubSelect = ParseSelect(input, offset, context, flags, null)
            };
            ResolveSelectReferences(source.SubSelect, context);
            offset += source.InputLength;

            next = NextToken(input, offset);
            var hasAs = false;
            if (next is { Kind: TokenKind.Keyword, KeywordValue: Keyword.As })
            {
                hasAs = true;
                source.SubSelect.InputLength += next.InputLength;
                offset += next.InputLength;
            }

            next = NextToken(input, offset);
            if (next.Kind == TokenKind.Symbol)
            {
                source.SubSelect.Identifier = next.StringValue;
                source.SubSelect.InputLength += next.InputLength;
            } else if (hasAs)
            {
                throw new WoobyParserException("Expected name after AS keyword", offset, next);
            }

            return source;
        }
        throw new WoobyParserException("Expected valid table reference or sub-select", offset, next);
    }

    private int ParseWhere(string input, int offset, Context context, Statement statement)
    {
        var originalOffset = offset;
        if (statement.Sources[0].Condition.Nodes.Count > 0)
        {
            throw new Exception("Unexpected WHERE when filter has already been set");
        }

        var exprFlags = new ExpressionFlags
        {
            GeneralWildcardAllowed = false, IdentifierAllowed = false, WildcardAllowed = false,
            SingleValueSubSelectAllowed = true, AllowAggregateFunctions = false
        };

        statement.Sources[0].Condition = ParseExpression(input, offset, context, statement, exprFlags, statement.Parent == null, false);
        offset += statement.Sources[0].Condition.FullText.Length;

        return offset - originalOffset;
    }

    private int ParseHaving(string input, int offset, Context context, SelectStatement statement)
    {
        var originalOffset = offset;
        if (statement.HavingCondition.Nodes.Count > 0)
        {
            throw new Exception("Unexpected HAVING when clause has already been set");
        }

        var exprFlags = new ExpressionFlags
        {
            GeneralWildcardAllowed = false,
            IdentifierAllowed = false,
            WildcardAllowed = false,
            SingleValueSubSelectAllowed = true,
            AllowAggregateFunctions = true
        };

        statement.HavingCondition = ParseExpression(input, offset, context, statement, exprFlags, statement.Parent == null, false);
        offset += statement.HavingCondition.FullText.Length;

        return offset - originalOffset;
    }
}