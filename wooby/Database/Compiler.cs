using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using wooby.Parsing;

namespace wooby.Database
{
    public static class Compiler
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
                Operator.Equal => OpCode.Eq,
                Operator.NotEqual => OpCode.NEq,
                Operator.LessThan => OpCode.Less,
                Operator.MoreThan => OpCode.More,
                Operator.LessEqual => OpCode.LessEq,
                Operator.MoreEqual => OpCode.MoreEq,
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

        public static void CompileExpression(SelectStatement command, Expression expr, Context context, List<Instruction> target)
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
            }
        }
    }
}
