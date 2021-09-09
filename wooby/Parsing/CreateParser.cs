﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace wooby.Parsing
{
    public partial class Parser
    {
        public CreateStatement ParseCreate(string input, int offset, Context context)
        {
            var statement = new CreateStatement();

            // First token is CREATE
            var next = NextToken(input, offset);
            offset += next.InputLength;
            // Second is the kind, but only TABLE is supported for now
            next = NextToken(input, offset);
            offset += next.InputLength;

            if (!next.IsKeyword() || next.KeywordValue != Keyword.Table)
            {
                throw new Exception("Unexpected value after CREATE");
            }

            next = NextToken(input, offset);
            offset += next.InputLength;

            if (next.Kind != TokenKind.Symbol)
            {
                throw new Exception("Expected name after CREATE TABLE");
            }

            statement.Name = next.StringValue;

            next = NextToken(input, offset);
            offset += next.InputLength;
            if (next.IsOperator() && next.OperatorValue != Operator.ParenthesisLeft)
            {
                throw new Exception("Expected ( after table name");
            }

            while (true)
            {
                if (offset >= input.Length)
                {
                    throw new Exception("Unexpected end of input");
                }

                next = NextToken(input, offset);

                if (next.IsOperator() && next.OperatorValue == Operator.ParenthesisRight)
                {
                    break;
                } else if (next.Kind == TokenKind.Comma)
                {
                    if (statement.Columns.Count == 0)
                    {
                        throw new Exception("Column list cannot start with a comma");
                    }

                    offset += next.InputLength;
                    next = NextToken(input, offset);
                }

                // Name of the column

                offset += next.InputLength;
                if (next.Kind != TokenKind.Symbol)
                {
                    throw new Exception("Expected column name");
                }

                var name = next.StringValue;

                var type = ParseColumnType(input, offset, out int delta);
                offset += delta;

                if (type == ColumnType.Null)
                {
                    throw new Exception($"Expected type for column '{name}'");
                }

                statement.Columns.Add(new ColumnNameTypeDef { Name = name, Type = type });
            }

            if (statement.Columns.Count == 0)
            {
                throw new Exception("Cannot create an empty table");
            }

            return statement;
        }
    }
}
