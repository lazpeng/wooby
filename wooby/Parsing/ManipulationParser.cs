using System;
using System.Collections.Generic;
using wooby.Error;

namespace wooby.Parsing;

public partial class Parser
{
    private List<string> ParseTargetColumnList(string input, int offset, out int length, Context context,
        Statement statement)
    {
        var originalOffset = offset;
        // ( was already skipped by caller

        var list = new List<string>();

        while (true)
        {
            var next = NextToken(input, offset);
            if (next.IsOperator() && next.OperatorValue == Operator.ParenthesisRight)
            {
                // We leave the ) for the caller
                break;
            }
            if (next.Kind == TokenKind.Comma)
            {
                if (list.Count == 0)
                {
                    throw new Exception("Column list starts with a comma");
                }
                offset += next.InputLength;
                next = NextToken(input, offset);
            }
            else if (next.Kind == TokenKind.None)
            {
                throw new Exception("Unexpected end of input");
            }

            offset += next.InputLength;
            AssertTokenIsSymbol(next, "Expected symbol in INSERT column target list");
            var column = next.StringValue;

            var mainSource = statement.Sources[0].Source;

            if (mainSource.FindReference(
                    new ColumnReference {Column = column, Table = mainSource.Table}, context) == null)
            {
                throw new Exception(
                    $"Expected valid existing column name in target columns list, found '{column}'");
            }

            list.Add(column);
        }

        length = offset - originalOffset;
        return list;
    }

    private List<Expression> ParseValuesList(string input, int offset, out int length, Context context,
        Statement statement)
    {
        var originalOffset = offset;
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
            if (next.Kind == TokenKind.Comma)
            {
                if (list.Count == 0)
                {
                    throw new Exception("Column list starts with a comma");
                }
                offset += next.InputLength;
            }
            else if (next.Kind == TokenKind.None)
            {
                throw new Exception("Unexpected end of input");
            }

            var expr = ParseExpression(input, offset, context, statement,
                new ExpressionFlags {SingleValueSubSelectAllowed = true}, true, false);
            list.Add(expr);
            offset += expr.FullText.Length;
        }

        length = offset - originalOffset;
        return list;
    }

    private List<Tuple<ColumnReference, Expression>> ParseUpdateSetColumns(string input, int offset, out int length,
        Context context, Statement statement)
    {
        var originalOffset = offset;
        var result = new List<Tuple<ColumnReference, Expression>>();
        var flags = new ReferenceFlags
            {AliasAllowed = false, ResolveReferences = true, TableOnly = false, WildcardAllowed = false};

        while (true)
        {
            var next = NextToken(input, offset);
            if (next.Kind == TokenKind.Keyword || next.Kind == TokenKind.None)
            {
                break;
            }
            if (next.Kind == TokenKind.Comma)
            {
                if (result.Count == 0)
                {
                    throw new Exception("Column list starts with a comma");
                }
                offset += next.InputLength;
            }

            var col = ParseReference(input, offset, context, statement, flags);
            offset += col.InputLength;
            next = NextToken(input, offset);
            offset += next.InputLength;
            AssertTokenIsOperator(next, Operator.Equal, "Expected = after column name");
            var expr = ParseExpression(input, offset, context, statement,
                new ExpressionFlags {SingleValueSubSelectAllowed = true}, true, false);
            offset += expr.FullText.Length;

            result.Add(new Tuple<ColumnReference, Expression>(col, expr));
        }

        length = offset - originalOffset;
        return result;
    }

    private InsertStatement ParseInsert(string input, int offset, Context context)
    {
        var originalOffset = offset;
        var statement = new InsertStatement();

        // First is INSERT, Next should be INTO
        SkipNextToken(input, ref offset);
        var next = NextToken(input, offset);
        AssertTokenIsKeyword(next, Keyword.Into, "Expected INTO after INSERT");
        offset += next.InputLength;

        // Which table we are inserting into
        var mainSource = ParseTableSource(input, offset, context, statement);
        statement.Sources.Add(new Joining {Kind = JoinKind.Inner, Source = mainSource});
        if (mainSource.Kind != TableSource.SourceKind.Reference)
        {
            throw new WoobyParserException("Expected table reference after FROM keyword", offset);
        }
        offset += mainSource.InputLength;

        next = NextToken(input, offset);
        offset += next.InputLength;

        if (next.IsOperator() && next.OperatorValue == Operator.ParenthesisLeft)
        {
            statement.Columns = ParseTargetColumnList(input, offset, out var length, context, statement);
            offset += length;

            // Next token should be a )
            SkipNextToken(input, ref offset);

            next = NextToken(input, offset);
            offset += next.InputLength;
        }

        AssertTokenIsKeyword(next, Keyword.Values, "Expected VALUES");
        next = NextToken(input, offset);
        AssertTokenIsOperator(next, Operator.ParenthesisLeft, "Expected a ( after VALUES");
        offset += next.InputLength;

        statement.Values = ParseValuesList(input, offset, out var len, context, statement);
        offset += len;

        // Next token should be a )
        SkipNextToken(input, ref offset);

        statement.OriginalText = input[originalOffset..offset];
        statement.InputLength = offset - originalOffset;
        return statement;
    }

    private UpdateStatement ParseUpdate(string input, int offset, Context context)
    {
        var originalOffset = offset;
        var statement = new UpdateStatement();

        // First is UPDATE, next is the table we're updating
        SkipNextToken(input, ref offset);
        var mainSource = ParseTableSource(input, offset, context, statement);
        statement.Sources.Add(new Joining {Kind = JoinKind.Inner, Source = mainSource});
        if (mainSource.Kind != TableSource.SourceKind.Reference)
        {
            throw new WoobyParserException("Expected table reference after FROM keyword", offset);
        }
        offset += mainSource.InputLength;
            
        var next = NextToken(input, offset);
        offset += next.InputLength;
        AssertTokenIsKeyword(next, Keyword.Set, "Expected SET after table name");

        statement.Columns = ParseUpdateSetColumns(input, offset, out var length, context, statement);
        offset += length;

        next = NextToken(input, offset);
        if (next.IsKeyword() && next.KeywordValue == Keyword.Where)
        {
            offset += next.InputLength;
            offset += ParseWhere(input, offset, context, statement);
        }
        else if (next.Kind == TokenKind.None)
        {
        } // Ok
        else
        {
            throw new Exception("Unexpected token after UPDATE");
        }

        statement.OriginalText = input[originalOffset..offset];
        statement.InputLength = offset - originalOffset;
        return statement;
    }

    private DeleteStatement ParseDelete(string input, int offset, Context context)
    {
        var originalOffset = offset;
        var statement = new DeleteStatement();

        // First is DELETE, Next should be FROM
        SkipNextToken(input, ref offset);
        var next = NextToken(input, offset);
        AssertTokenIsKeyword(next, Keyword.From, "Expected FROM after DELETE");
        offset += next.InputLength;

        // Which table we are deleting from
        var mainSource = ParseTableSource(input, offset, context, statement);
        statement.Sources.Add(new Joining {Kind = JoinKind.Inner, Source = mainSource});
        if (mainSource.Kind != TableSource.SourceKind.Reference)
        {
            throw new WoobyParserException("Expected table reference after FROM keyword", offset);
        }
        offset += mainSource.InputLength;

        next = NextToken(input, offset);
        if (next.IsKeyword() && next.KeywordValue == Keyword.Where)
        {
            offset += next.InputLength;
            offset += ParseWhere(input, offset, context, statement);
        }
        else if (next.Kind == TokenKind.None)
        {
        } // Ok
        else
        {
            throw new Exception("Unexpected token after DELETE");
        }

        statement.OriginalText = input[originalOffset..offset];
        statement.InputLength = offset - originalOffset;
        return statement;
    }
}