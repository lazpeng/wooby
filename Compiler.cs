using System;
using System.Collections.Generic;
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
        By
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
        }

        public enum ExpressionType
        {
            Unknown,
            Number,
            String,
            Boolean
        }

        private string _identifier;

        public string FullText { get; set; }
        public string Identifier
        {
            get
            {
                return _identifier ?? FullText;
            }
            set
            {
                _identifier = value;
            }
        }
        public List<Node> Nodes { get; set; } = new List<Node>();
        public ExpressionType TypeGuess { get; set; } = ExpressionType.Unknown;
    }

    public class TableReference
    {
        public string Schema { get; set; } = "";
        public string Table { get; set; } = "";
        public int InputLength { get; set; }
    }

    public class ColumnReference : TableReference
    {
        public string Column { get; set; } = "";
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
    }

    public class Compiler
    {

    }
}
