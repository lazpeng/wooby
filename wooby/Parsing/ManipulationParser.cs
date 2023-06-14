using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace wooby.Parsing
{
    public partial class Parser
    {
        private List<string> ParseTargetColumnList(string input, int offset, out int length, Context context, InsertStatement statement)
        {
            int originalOffset = offset;
            // ( was already skipped by caller

            var list = new List<string>();

            while (true)
            {
                var next = NextToken(input, offset);
                if (next.IsOperator() && next.OperatorValue == Operator.ParenthesisRight)
                {
                    // We leave the ) for the caller
                    break;
                } else if (next.Kind == TokenKind.Comma)
                {
                    if (list.Count == 0)
                    {
                        throw new Exception("Column list starts with a comma");
                    } else
                    {
                        offset += next.InputLength;
                        next = NextToken(input, offset);
                    }
                }

                AssertTokenIsSymbol(next, "Expected symbol in INSERT column target list");
                var column = next.StringValue;

                if (context.FindColumn(new ColumnReference { Column = column, Table = statement.MainSource.Table }) == null)
                {
                    throw new Exception($"Expected valid existing column name in target columns list, found '{column}'");
                }

                list.Add(column);
            }

            length = offset - originalOffset;
            return list;
        }

        private List<Expression> ParseValuesList(string input, int offset, out int length, Context context, InsertStatement statement)
        {
            int originalOffset = offset;
            // ( was already skipped by caller

            var list = new List<Expression>();

            while (true)
            {
                var next = NextToken(input, offset);
                if (next.IsOperator() && next.OperatorValue == Operator.ParenthesisRight)
                {
                    // We leave the ) for the caller
                    break;
                }
                else if (next.Kind == TokenKind.Comma)
                {
                    if (list.Count == 0)
                    {
                        throw new Exception("Column list starts with a comma");
                    }
                    else
                    {
                        offset += next.InputLength;
                    }
                }

                list.Add(ParseExpression(input, offset, context, statement, new ExpressionFlags { SingleValueSubSelectAllowed = true }, true, false));
            }

            length = offset - originalOffset;
            return list;
        }

        public InsertStatement ParseInsert(string input, int offset, Context context)
        {
            int originalOffset = offset;
            var statement = new InsertStatement();

            // First is INSERT, Next should be INTO
            SkipNextToken(input, ref offset);
            var next = NextToken(input, offset);
            AssertTokenIsKeyword(next, Keyword.Into, "Expected INTO after INSERT");
            offset += next.InputLength;

            // Which table we are INSERTing into
            statement.MainSource = ParseReference(input, offset, context, statement, new ReferenceFlags { TableOnly = true });

            next = NextToken(input, offset);
            offset += next.InputLength;

            if (next.IsOperator() && next.OperatorValue == Operator.ParenthesisLeft)
            {
                statement.Columns = ParseTargetColumnList(input, offset, out int length, context, statement);
                offset += length;

                // Next token should be a )
                SkipNextToken(input, ref offset);

                next = NextToken(input, offset);
            }

            AssertTokenIsKeyword(next, Keyword.Values, "Expected VALUES");
            offset += next.InputLength;

            next = NextToken(input, offset);
            AssertTokenIsOperator(next, Operator.ParenthesisLeft, "Expected a ( after VALUES");
            offset += next.InputLength;

            statement.Values = ParseValuesList(input, offset, out int len, context, statement);
            offset += len;

            // Next token should be a )
            SkipNextToken(input, ref offset);

            statement.OriginalText = input[originalOffset..offset];
            return statement;
        }

        public UpdateStatement ParseUpdate(string input, int offset, Context context)
        {
            throw new NotImplementedException();
        }

        public DeleteStatement ParseDelete(string input, int offset, Context context)
        {
            throw new NotImplementedException();
        }
    }
}
