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
                        inst.Arg3 = node.ReferenceValue.ParentLevel;
                    }
                    else
                    {
                        throw new InvalidOperationException("Reference could not be found");
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
                _ => throw new ArgumentException("Invalid operator")
            };
        }

        private static void CompileFunctionCall(FunctionCall call, Context context, List<Instruction> target)
        {
            // Compile arguments in reverse order

            for (int i = call.Arguments.Count - 1; i >= 0; --i)
            {
                CompileSubExpression(0, call.Arguments[i], context, target);
            }

            var func = context.FindFunction(call.Name);

            target.Add(new Instruction() { OpCode = OpCode.CallFunction, Arg1 = func.Id, Arg2 = call.Arguments.Count });
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
                    if (node.Kind == Expression.NodeKind.Function)
                    {
                        CompileFunctionCall(node.FunctionCall, context, target);
                    }
                    else if (node.Kind == Expression.NodeKind.SubSelect)
                    {
                        target.Add(new Instruction() { OpCode = OpCode.ExecuteSubQuery, SubQuery = node.SubSelect });
                    }
                    else
                    {
                        temp.Add(GetValuePushInstruction(node, context));
                    }
                    temp.Add(new Instruction() { OpCode = GetOpcodeForOperator(opStack.Pop()) });
                    lastWasPrecedence = false;
                }
                else if (node.Kind == Expression.NodeKind.Operator)
                {
                    if (node.OperatorValue == Operator.ParenthesisLeft)
                    {
                        i = CompileSubExpression(i + 1, expr, context, target);
                    }
                    else if (node.OperatorValue == Operator.ParenthesisRight)
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
                    if (node.Kind == Expression.NodeKind.Function)
                    {
                        CompileFunctionCall(node.FunctionCall, context, target);
                    }
                    else if (node.Kind == Expression.NodeKind.SubSelect)
                    {
                        target.Add(new Instruction() { OpCode = OpCode.ExecuteSubQuery, SubQuery = node.SubSelect });
                    }
                    else
                    {
                        temp.Add(GetValuePushInstruction(node, context));
                    }
                }
            }

            while (opStack.Count > 0)
            {
                temp.Add(new Instruction() { OpCode = GetOpcodeForOperator(opStack.Pop()) });
            }

            target.AddRange(temp);
            return i;
        }

        public static void CompileExpression(SelectStatement command, Expression expr, Context context, List<Instruction> target, PushResultKind push)
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
                        var variable = context.FindFunction(reference.Join());
                        target.Add(new Instruction() { OpCode = OpCode.CallFunction, Arg1 = variable.Id });
                    }
                    else
                    {
                        var column = context.FindColumn(reference);

                        target.Add(new Instruction()
                        {
                            OpCode = OpCode.PushColumn,
                            Arg1 = column.Parent.Id,
                            Arg2 = column.Id
                        });

                        OpCode op;
                        if (push == PushResultKind.ToOrdering)
                        {
                            op = OpCode.PushStackTopToOrdering;
                        }
                        else
                        {
                            op = OpCode.PushStackTopToOutput;
                        }

                        target.Add(new Instruction()
                        {
                            OpCode = op,
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
                if (push == PushResultKind.ToOutput)
                {
                    target.Add(new Instruction() { OpCode = OpCode.PushStackTopToOutput });
                }
                else if (push == PushResultKind.ToOrdering)
                {
                    target.Add(new Instruction() { OpCode = OpCode.PushStackTopToOrdering });
                }
            }
        }
    }
}
