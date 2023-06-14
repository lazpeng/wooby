using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
        As
    }

    public class Expression
    {
        public enum NodeKind
        {
            Operator,
            String,
            Number,
            Reference,
            Null
        }

        public class Node
        {
            public NodeKind Kind;
            public string StringValue;
            public double NumberValue;
            public Operator OperatorValue;
            public ColumnReference ReferenceValue;

            public override bool Equals(object obj)
            {
                return obj is Node node &&
                       Kind == node.Kind &&
                       StringValue == node.StringValue &&
                       NumberValue == node.NumberValue &&
                       OperatorValue == node.OperatorValue &&
                       (ReferenceValue == node.ReferenceValue || ReferenceValue.Equals(node.ReferenceValue));
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
            Boolean
        }

        public static ExpressionType ColumnTypeToExpressionType(ColumnType type)
        {
            return type switch
            {
                ColumnType.Boolean => ExpressionType.Boolean,
                ColumnType.Number => ExpressionType.Number,
                ColumnType.String => ExpressionType.String,
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

    public class ColumnReference
    {
        public string Table { get; set; } = "";
        public string Column { get; set; } = "";
        public string Identifier { get; set; } = "";
        public int InputLength { get; set; }

        public override bool Equals(object obj)
        {
            return obj is ColumnReference reference &&
                    Table == reference.Column &&
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
        Pragma,
        Query,
        Manipulation,
        Definition
    }

    public enum StatementClass
    {
        Select,
        Update,
        Delete,
        Alter,
        Create,
    }

    public abstract class Statement
    {
        public StatementKind Kind { get; protected set; }
        public StatementClass Class { get; protected set; }
        public string OriginalText { get; set; }
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
            return obj is Ordering ordering &&
                   (OrderExpression == ordering.OrderExpression || OrderExpression.Equals(OrderExpression)) &&
                   Kind == ordering.Kind;
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
        public ColumnReference MainSource { get; set; }
        public Expression FilterConditions { get; set; }
        public Ordering OutputOrder { get; set; }

        public override bool Equals(object obj)
        {
            if (obj is SelectStatement statement)
            {
                return Kind == statement.Kind &&
                   Class == statement.Class &&
                   OutputColumns.SequenceEqual(statement.OutputColumns) &&
                   (MainSource == statement.MainSource || MainSource.Equals(statement.MainSource)) &&
                   (FilterConditions == statement.FilterConditions || FilterConditions.Equals(statement.FilterConditions)) &&
                   (OutputOrder == statement.OutputOrder || OutputOrder.Equals(statement.OutputOrder));
            }
            else return false;
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(Kind, Class, OutputColumns, MainSource, FilterConditions, OutputOrder);
        }
    }
}
