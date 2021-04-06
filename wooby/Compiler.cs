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
        public ColumnReference MainSource { get; set; }
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
        private static Instruction GetValuePushInstruction(Expression.Node node, Context context)
        {
            var inst = new Instruction();

            switch (node.Kind)
            {
                case Expression.NodeKind.Number:
                    inst.Num1 = node.NumberValue;
                    inst.OpCode = OpCode.PushNumber;
                    break;
                case Expression.NodeKind.String:
                    inst.Str1 = node.StringValue;
                    inst.OpCode = OpCode.PushString;
                    break;
                case Expression.NodeKind.Reference:
                    var col = context.FindColumn(node.ReferenceValue);
                    if (col != null)
                    {
                        inst.OpCode = OpCode.PushColumn;
                        inst.Arg1 = col.Parent.Id;
                        inst.Arg2 = col.Id;
                    } else
                    {
                        var v = context.FindVariable(node.ReferenceValue.Column);
                        if (v != null)
                        {
                            inst.OpCode = OpCode.PushVariable;
                            inst.Arg1 = v.Id;
                        } else throw new InvalidOperationException();
                    }
                    break;
            }

            return inst;
        }

        private static OpCode GetOpcodeForOperator(Operator op)
        {
            return op switch
            {
                Operator.Asterisk => OpCode.Mul,
                Operator.Plus => OpCode.Sum,
                Operator.Minus => OpCode.Sub,
                Operator.ForwardSlash => OpCode.Div,
                _ => throw new ArgumentException()
            };
        }

        private static int CompileSubExpression(int offset, Expression expr, Context context, List<Instruction> target)
        {
            var temp = new List<Instruction>();

            var opStack = new Stack<Operator>();
            var lastWasPrecedence = false;

            int i;
            for (i = offset; i < expr.Nodes.Count; ++i)
            {
                var node = expr.Nodes[i];

                if (lastWasPrecedence)
                {
                    temp.Add(GetValuePushInstruction(node, context));
                    temp.Add(new Instruction() { OpCode = GetOpcodeForOperator(opStack.Pop()) });
                }
                else if (node.Kind == Expression.NodeKind.Operator)
                {
                    if (node.OperatorValue == Operator.ParenthesisLeft)
                    {
                        i = CompileSubExpression(i + 1, expr, context, target);
                    } else if (node.OperatorValue == Operator.ParenthesisRight)
                    {
                        break;
                    }

                    opStack.Push(node.OperatorValue);

                    switch (node.OperatorValue)
                    {
                        case Operator.Asterisk:
                        case Operator.ForwardSlash:
                            lastWasPrecedence = true;
                            break;
                        default:
                            break;
                    }
                }
                else
                {
                    temp.Add(GetValuePushInstruction(node, context));
                }
            }

            while (opStack.Count > 0)
            {
                temp.Add(new Instruction() { OpCode = GetOpcodeForOperator(opStack.Pop()) });
            }

            target.AddRange(temp);
            return i;
        }

        private static void CompileExpression(SelectCommand command, Expression expr, Context context, List<Instruction> target)
        {
            if (expr.IsOnlyReference())
            {
                var reference = expr.Nodes[0].ReferenceValue;

                if (expr.IsWildcard())
                {
                    var table = context.FindTable(reference);

                    foreach (var col in table.Columns)
                    {
                        target.Add(new Instruction()
                        {
                            OpCode = OpCode.PushColumnToOutput,
                            Arg1 = col.Parent.Id,
                            Arg2 = col.Id
                        });
                    }
                }
                else
                {
                    if (string.IsNullOrEmpty(reference.Table))
                    {
                        var variable = context.FindVariable(reference.Join());
                        target.Add(new Instruction() { OpCode = OpCode.PushVariableToOutput, Arg1 = variable.Id });
                    }
                    else
                    {
                        var column = context.FindColumn(reference);
                        target.Add(new Instruction()
                        {
                            OpCode = OpCode.PushColumnToOutput,
                            Arg1 = column.Parent.Id,
                            Arg2 = column.Id
                        });
                    }
                }
            }
            else if (expr.IsWildcard())
            {
                // Push columns for all tables in select command

                foreach (var col in context.FindTable(command.MainSource).Columns)
                {
                    target.Add(new Instruction()
                    {
                        OpCode = OpCode.PushColumnToOutput,
                        Arg1 = col.Parent.Id,
                        Arg2 = col.Id
                    });
                }
            }
            else
            {
                CompileSubExpression(0, expr, context, target);

                target.Add(new Instruction() { OpCode = OpCode.PushStackTopToOutput });
            }
        }

        public static void CompileSelectCommand(SelectCommand command, Context context, List<Instruction> target)
        {
            var sourceTable = context.FindTable(command.MainSource);
            if (sourceTable == null)
            {
                throw new InvalidOperationException("Invalid reference to main source of select command");
            }

            target.Add(new Instruction() { OpCode = OpCode.SelectSourceTable, Arg1 = sourceTable.Id });

            foreach (var expr in command.OutputColumns)
            {
                if (expr.IsWildcard())
                {
                    if (expr.IsOnlyReference())
                    {
                        var reference = expr.Nodes[0].ReferenceValue;

                        var table = context.FindTable(reference);

                        foreach (var col in table.Columns)
                        {
                            target.Add(new Instruction()
                            {
                                OpCode = OpCode.AddOutputColumnDefinition,
                                Str1 = col.Name
                            });
                        }
                    }
                    else
                    {
                        // Push columns for all tables in select command

                        foreach (var col in context.FindTable(command.MainSource).Columns)
                        {
                            target.Add(new Instruction()
                            {
                                OpCode = OpCode.AddOutputColumnDefinition,
                                Str1 = col.Name
                            });
                        }
                    }
                }
                else
                {
                    var id = expr.Identifier;
                    if (string.IsNullOrEmpty(id))
                    {
                        id = expr.FullText;
                    }
                    target.Add(new Instruction() { OpCode = OpCode.AddOutputColumnDefinition, Str1 = id });
                }
            }

            target.Add(new Instruction() { OpCode = OpCode.PushCheckpointNext });
            target.Add(new Instruction() { OpCode = OpCode.TrySeekElseSkip });
            target.Add(new Instruction() { OpCode = OpCode.NewOutputRow });

            foreach (var expr in command.OutputColumns)
            {
                CompileExpression(command, expr, context, target);
            }

            target.Add(new Instruction() { OpCode = OpCode.CheckpointEnd });

            // TODO Filter conditions

            // TODO Ordering
        }

        public static List<Instruction> CompileCommand(Command command, Context context)
        {
            var list = new List<Instruction>();

            if (command is SelectCommand select)
            {
                CompileSelectCommand(select, context, list);
            }

            return list;
        }
    }
}
