using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace wooby
{
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
        Equal,
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
            Boolean,
            Reference
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
                       ReferenceValue == node.ReferenceValue;
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
        public ColumnReference Identifier { get; set; }
        public List<Node> Nodes { get; set; } = new List<Node>();
        public ExpressionType Type { get; set; } = ExpressionType.Unknown;
        public bool IsBoolean { get; set; } = false;

        public bool IsWildcard()
        {
            if (Nodes.Count == 0)
            {
                return false;
            }

            var tokens = Nodes.Where(p => p.Kind == NodeKind.Operator && !(p.OperatorValue == Operator.ParenthesisLeft || p.OperatorValue == Operator.ParenthesisRight));
            return tokens.Any() && tokens.First().IsWildcard();
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

    public class TableReference
    {
        public string Schema { get; set; } = "";
        public string Table { get; set; } = "";
        public int InputLength { get; set; }

        public override bool Equals(object obj)
        {
            return obj is TableReference reference &&
                   Schema == reference.Schema &&
                   Table == reference.Table;
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(Schema, Table);
        }
    }

    public class ColumnReference : TableReference
    {
        public string Column { get; set; } = "";

        public override bool Equals(object obj)
        {
            return obj is ColumnReference reference &&
                    base.Equals(obj) &&
                    Column == reference.Column;
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(base.GetHashCode(), Schema, Table, Column);
        }
    }

    public enum CommandKind
    {
        Pragma,
        Query,
        Manipulation,
        Definition
    }

    public enum CommandClass
    {
        Select,
        Update,
        Delete,
        Alter,
        Create,
    }

    public abstract class Command
    {
        public CommandKind Kind { get; protected set; }
        public CommandClass Class { get; protected set; }
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

    public class SelectCommand : Command
    {
        public SelectCommand()
        {
            Kind = CommandKind.Query;
            Class = CommandClass.Select;
        }

        public List<Expression> OutputColumns { get; private set; } = new List<Expression>();
        public TableReference MainSource { get; set; }
        public Expression FilterConditions { get; set; }
        public Ordering OutputOrder { get; set; }

        public override bool Equals(object obj)
        {
            if (obj is SelectCommand command)
            {
                return Kind == command.Kind &&
                   Class == command.Class &&
                   OutputColumns.SequenceEqual(command.OutputColumns) &&
                   (MainSource == command.MainSource || MainSource.Equals(command.MainSource)) &&
                   (FilterConditions == command.FilterConditions || FilterConditions.Equals(command.FilterConditions)) &&
                   (OutputOrder == command.OutputOrder || OutputOrder.Equals(command.OutputOrder));
            }
            else return false;
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(Kind, Class, OutputColumns, MainSource, FilterConditions, OutputOrder);
        }
    }

    public class Compiler
    {

    }
}
