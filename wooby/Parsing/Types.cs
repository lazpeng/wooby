using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static wooby.Parsing.Parser;

namespace wooby.Parsing
{
    partial class Parser
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

        public class Token
        {
            public TokenKind Kind;
            public string StringValue;
            public double NumberValue;
            public Keyword KeywordValue;
            public Operator OperatorValue;
            public int InputLength;

            public bool IsOperator()
            {
                return Kind == TokenKind.Operator;
            }

            public bool IsKeyword()
            {
                return Kind == TokenKind.Keyword;
            }
        }

        public class ExpressionFlags
        {
            // This is for any wildcard, including the syntax tablename.*
            public bool WildcardAllowed = false;
            // This is for the single * character, disallowed when a column has already been specified
            public bool GeneralWildcardAllowed = false;
            // Whether or not you can alias the expression to an identifier
            public bool IdentifierAllowed = false;
            // If a single-value subselect is allowed
            public bool SingleValueSubSelectAllowed = false;
        }

        public class StatementFlags
        {
            // If the select should only return a single column
            public bool SingleValueReturn = false;
            // If the resulting query can stop in a unmatched parenthesis (or throw an error)
            public bool StopOnUnmatchedParenthesis = false;
        }

        public class ReferenceFlags
        {
            public bool WildcardAllowed = false;
            public bool ResolveReferences = false;
            public bool AliasAllowed = false;
            public bool TableOnly = false;
        }
    }

    public enum Operator
    {
        Plus,
        Minus,
        ForwardSlash,
        Asterisk,
        ParenthesisLeft,
        ParenthesisRight,
        Power,
        LessThan,
        MoreThan,
        LessEqual,
        MoreEqual,
        Equal,
        NotEqual,
        Remainder,
    }

    public enum Keyword
    {
        Select,
        From,
        Where,
        Is,
        Not,
        Null,
        True,
        False,
        And,
        Or,
        Asc,
        Desc,
        Order,
        By,
        As,
        Create,
        Insert,
        Update,
        Delete,
        Alter,
        Set,
        Into,
        Values,
        Table,
        Column,
        Add,
        Constraint,
    }

    public class Expression
    {
        public enum NodeKind
        {
            Operator,
            String,
            Number,
            Reference,
            Function,
            SubSelect,
            Null
        }

        public class Node
        {
            public NodeKind Kind;
            public string StringValue;
            public double NumberValue;
            public Operator OperatorValue;
            public ColumnReference ReferenceValue;
            public FunctionCall FunctionCall;
            public SelectStatement SubSelect;

            public override bool Equals(object obj)
            {
                if (obj is Node node)
                {
                    return Kind == node.Kind &&
                       StringValue == node.StringValue &&
                       NumberValue == node.NumberValue &&
                       OperatorValue == node.OperatorValue &&
                       (ReferenceValue == node.ReferenceValue || ReferenceValue.Equals(node.ReferenceValue)) &&
                       (FunctionCall == node.FunctionCall || FunctionCall.Equals(node.FunctionCall)) &&
                       SubSelect == node.SubSelect || SubSelect.Equals(node.SubSelect);
                }
                else return false;
            }

            public override int GetHashCode()
            {
                return HashCode.Combine(Kind, StringValue, NumberValue, OperatorValue, ReferenceValue);
            }

            public bool IsWildcard()
            {
                return (Kind == NodeKind.Operator && OperatorValue == Operator.Asterisk) || (Kind == NodeKind.Reference && ReferenceValue.Column == "*");
            }
        }

        public enum ExpressionType
        {
            Unknown,
            Number,
            String,
            Boolean,
            Null,
            Date
        }

        public static ExpressionType ColumnTypeToExpressionType(ColumnType type)
        {
            return type switch
            {
                ColumnType.Boolean => ExpressionType.Boolean,
                ColumnType.Number => ExpressionType.Number,
                ColumnType.String => ExpressionType.String,
                ColumnType.Date => ExpressionType.Date,
                _ => throw new NotImplementedException()
            };
        }

        public string FullText { get; set; }
        public string Identifier { get; set; }
        public List<Node> Nodes { get; set; } = new List<Node>();
        public ExpressionType Type { get; set; } = ExpressionType.Unknown;
        public bool IsBoolean { get; set; } = false;

        public bool IsWildcard()
        {
            if (Nodes.Count == 0)
            {
                return false;
            }

            var nodes = Nodes.Where(p => (p.Kind == NodeKind.Operator && !(p.OperatorValue == Operator.ParenthesisLeft || p.OperatorValue == Operator.ParenthesisRight)) || p.Kind == NodeKind.Reference);
            return nodes.Any() && nodes.First().IsWildcard();
        }

        public bool IsOnlyReference()
        {
            return Nodes.Count == 1 && Nodes[0].Kind == NodeKind.Reference;
        }

        public bool IsOnlyFunctionCall()
        {
            return Nodes.Count == 1 && Nodes[0].Kind == NodeKind.Function;
        }

        public override bool Equals(object obj)
        {
            if (obj is Expression expression)
            {
                return Identifier == expression.Identifier &&
                       Nodes.SequenceEqual(expression.Nodes) &&
                       Type == expression.Type &&
                       IsBoolean == expression.IsBoolean;
            }
            else return false;
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(FullText, Identifier, Nodes, Type);
        }
    }

    public class FunctionCall
    {
        public string Name { get; set; }
        public List<Expression> Arguments { get; set; }

        public override bool Equals(object obj)
        {
            return obj is FunctionCall call &&
                   Name == call.Name &&
                   Arguments.SequenceEqual(call.Arguments);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(Name, Arguments);
        }
    }

    public class ColumnReference
    {
        public string Table { get; set; } = "";
        public string Column { get; set; } = "";
        public string Identifier { get; set; } = "";
        public string TableIdentifier { get; set; } = "";
        public int InputLength { get; set; }
        public int ParentLevel { get; set; } = 0;

        public override bool Equals(object obj)
        {
            return obj is ColumnReference reference &&
                    Table == reference.Table &&
                    Column == reference.Column;
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(base.GetHashCode(), Table, Column);
        }

        public string Join()
        {
            // Column is guaranteed to be non empty
            if (string.IsNullOrEmpty(Table))
            {
                return Column;
            }
            else return $"{Table}.{Column}";
        }
    }

    public enum StatementKind
    {
        Query,
        Manipulation,
        Definition
    }

    public enum StatementClass
    {
        Select,
        Insert,
        Update,
        Delete,
        Alter,
        Create,
        Drop,
    }

    public abstract class Statement
    {
        public StatementKind Kind { get; protected set; }
        public StatementClass Class { get; protected set; }
        public string OriginalText { get; set; }

        public ColumnReference MainSource { get; set; }
        public Expression FilterConditions { get; set; }
        public Statement Parent { get; set; } = null;
        public StatementFlags UsedFlags { get; set; } = new StatementFlags();

        public ColumnReference TryFindReferenceRecursive(Context context, ColumnReference reference, int level)
        {
            if (reference.Table == MainSource.Table || reference.Table == MainSource.Identifier || reference.Table == "")
            {
                var col = context.FindColumn(new ColumnReference { Table = MainSource.Table, Column = reference.Column });
                if (col != null)
                {
                    reference.Table = MainSource.Table;
                    reference.TableIdentifier = MainSource.Identifier;
                    reference.ParentLevel = level;
                    return reference;
                }
            }

            if (Parent != null)
            {
                return Parent.TryFindReferenceRecursive(context, reference, level + 1);
            }
            else return null;
        }
    }

    public enum OrderingKind
    {
        Ascending,
        Descending
    }

    public class Ordering
    {
        public Expression OrderExpression { get; set; }
        public OrderingKind Kind { get; set; } = OrderingKind.Ascending;

        public override bool Equals(object obj)
        {
            if (obj is Ordering ordering)
            {
                return (OrderExpression == ordering.OrderExpression || OrderExpression.Equals(OrderExpression)) &&
                   Kind == ordering.Kind;
            } else return false;
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(OrderExpression, Kind);
        }
    }

    public class SelectStatement : Statement
    {
        public SelectStatement()
        {
            Kind = StatementKind.Query;
            Class = StatementClass.Select;
        }

        public List<Expression> OutputColumns { get; private set; } = new List<Expression>();
        public List<Ordering> OutputOrder { get; set; } = new List<Ordering>();
        public string Identifier { get; set; } = "";

        public override bool Equals(object obj)
        {
            if (obj is SelectStatement statement)
            {
                return Kind == statement.Kind &&
                   Class == statement.Class &&
                   OutputColumns.SequenceEqual(statement.OutputColumns) &&
                   (MainSource == statement.MainSource || MainSource.Equals(statement.MainSource)) &&
                   (FilterConditions == statement.FilterConditions || FilterConditions.Equals(statement.FilterConditions)) &&
                   OutputOrder.SequenceEqual(statement.OutputOrder);
            }
            else return false;
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(Kind, Class, OutputColumns, MainSource, FilterConditions, OutputOrder);
        }
    }

    public class ColumnNameTypeDef
    {
        public string Name { get; set; }
        public ColumnType Type { get; set; }
    }

    public class CreateStatement : Statement
    {
        public CreateStatement()
        {
            Kind = StatementKind.Definition;
            Class = StatementClass.Create;
        }

        public string Name { get; set; }
        public List<ColumnNameTypeDef> Columns { get; set; } = new List<ColumnNameTypeDef>();
    }

    public class InsertStatement : Statement
    {
        public InsertStatement()
        {
            Kind = StatementKind.Manipulation;
            Class = StatementClass.Insert;
        }

        public List<string> Columns { get; set; } = new List<string>();
        public List<Expression> Values { get; set; } = new List<Expression>();
    }

    public class UpdateStatement : Statement
    {
        public UpdateStatement()
        {
            Kind = StatementKind.Manipulation;
            Class = StatementClass.Update;
        }

        public List<Tuple<ColumnReference, Expression>> Columns { get; set; } = new List<Tuple<ColumnReference, Expression>>();
    }

    public class DeleteStatement : Statement
    {
        public DeleteStatement()
        {
            Kind = StatementKind.Manipulation;
            Class = StatementClass.Update;
        }
    }
}
