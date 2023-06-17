using System;
using System.Collections.Generic;
using System.Linq;
using wooby.Database.Defaults;
using wooby.Database.Persistence;
using wooby.Error;

namespace wooby.Database
{
    public enum ValueKind
    {
        Text,
        Number,
        Boolean,
        Date,
        Null,
    }

    public abstract record BaseValue
    {
        public abstract string PrettyPrint();
        public abstract BaseValue Add(BaseValue other);
        public abstract BaseValue Subtract(BaseValue other);
        public abstract BaseValue Divide(BaseValue other);
        public abstract BaseValue Multiply(BaseValue other);
        public abstract BaseValue Power(BaseValue other);
        public abstract BaseValue Remainder(BaseValue other);
        public abstract BaseValue And(BaseValue other);
        public abstract BaseValue Or(BaseValue other);
        public abstract int Compare(BaseValue other);
    }

    public record TextValue : BaseValue
    {
        public string Value { get; private set; }

        public TextValue(string value)
        {
            Value = value;
        }

        public override string PrettyPrint()
        {
            return Value;
        }

        public override BaseValue Add(BaseValue other)
        {
            if (other is TextValue otherText)
            {
                return new TextValue(Value + otherText.Value);
            }
            else throw new WoobyIncompatibleTypesException(this, other);
        }

        public override BaseValue Subtract(BaseValue other)
        {
            throw new WoobyIncompatibleTypesException(this, other);
        }

        public override BaseValue Divide(BaseValue other)
        {
            throw new WoobyIncompatibleTypesException(this, other);
        }

        public override BaseValue Multiply(BaseValue other)
        {
            throw new WoobyIncompatibleTypesException(this, other);
        }

        public override BaseValue Power(BaseValue other)
        {
            throw new WoobyIncompatibleTypesException(this, other);
        }

        public override BaseValue Remainder(BaseValue other)
        {
            throw new WoobyIncompatibleTypesException(this, other);
        }

        public override BaseValue And(BaseValue other)
        {
            throw new WoobyIncompatibleTypesException(this, other);
        }

        public override BaseValue Or(BaseValue other)
        {
            throw new WoobyIncompatibleTypesException(this, other);
        }

        public override int Compare(BaseValue other)
        {
            // Does not compare if greater or lesser, only equals or not
            if (other is TextValue otherText)
            {
                return Value == otherText.Value ? 0 : -1;
            }
            else if (other is NullValue)
            {
                // FIXME: Remove when IS / IS NOT is implemented
                return -1;
            }
            else throw new WoobyIncompatibleTypesException(this, other);
        }
    }

    public record NumberValue : BaseValue
    {
        public double Value { get; private set; }

        public NumberValue(double value)
        {
            Value = value;
        }

        public override string PrettyPrint()
        {
            return $"{Value}";
        }

        public override BaseValue Add(BaseValue other)
        {
            if (other is NumberValue num)
            {
                return new NumberValue(Value + num.Value);
            }
            else throw new WoobyIncompatibleTypesException(this, other);
        }

        public override BaseValue Subtract(BaseValue other)
        {
            if (other is NumberValue num)
            {
                return new NumberValue(Value - num.Value);
            }
            else throw new WoobyIncompatibleTypesException(this, other);
        }

        public override BaseValue Divide(BaseValue other)
        {
            if (other is NumberValue num)
            {
                return new NumberValue(Value / num.Value);
            }
            else throw new WoobyIncompatibleTypesException(this, other);
        }

        public override BaseValue Multiply(BaseValue other)
        {
            if (other is NumberValue num)
            {
                return new NumberValue(Value * num.Value);
            }
            else throw new WoobyIncompatibleTypesException(this, other);
        }

        public override BaseValue Power(BaseValue other)
        {
            if (other is NumberValue num)
            {
                return new NumberValue(Math.Pow(Value, num.Value));
            }
            else throw new WoobyIncompatibleTypesException(this, other);
        }

        public override BaseValue Remainder(BaseValue other)
        {
            if (other is NumberValue num)
            {
                return new NumberValue(Value % num.Value);
            }
            else throw new WoobyIncompatibleTypesException(this, other);
        }

        public override BaseValue And(BaseValue other)
        {
            throw new WoobyIncompatibleTypesException(this, other);
        }

        public override BaseValue Or(BaseValue other)
        {
            throw new WoobyIncompatibleTypesException(this, other);
        }

        public override int Compare(BaseValue other)
        {
            if (other is NumberValue num)
            {
                if (Value < num.Value)
                {
                    return -1;
                }
                else if (Value > num.Value)
                {
                    return 1;
                }
                else
                {
                    return 0;
                }
            }
            else if (other is NullValue)
            {
                // FIXME: Remove when IS / IS NOT is implemented
                return -1;
            }
            else throw new WoobyIncompatibleTypesException(this, other);
        }
    }

    public record NullValue : BaseValue
    {
        public override string PrettyPrint()
        {
            return "";
        }

        public override BaseValue Add(BaseValue other)
        {
            throw new WoobyIncompatibleTypesException(this, other);
        }

        public override BaseValue Subtract(BaseValue other)
        {
            throw new WoobyIncompatibleTypesException(this, other);
        }

        public override BaseValue Divide(BaseValue other)
        {
            throw new WoobyIncompatibleTypesException(this, other);
        }

        public override BaseValue Multiply(BaseValue other)
        {
            throw new WoobyIncompatibleTypesException(this, other);
        }

        public override BaseValue Power(BaseValue other)
        {
            throw new WoobyIncompatibleTypesException(this, other);
        }

        public override BaseValue Remainder(BaseValue other)
        {
            throw new WoobyIncompatibleTypesException(this, other);
        }

        public override BaseValue And(BaseValue other)
        {
            throw new WoobyIncompatibleTypesException(this, other);
        }

        public override BaseValue Or(BaseValue other)
        {
            throw new WoobyIncompatibleTypesException(this, other);
        }

        public override int Compare(BaseValue other)
        {
            if (other is NullValue)
            {
                return 0;
            }

            return -1;
        }
    }

    public record BooleanValue : NullValue
    {
        public bool Value { get; private set; }

        public void Flip()
        {
            Value = !Value;
        }

        public override string PrettyPrint()
        {
            return Value ? "TRUE" : "FALSE";
        }

        public BooleanValue(bool value)
        {
            Value = value;
        }

        public override BaseValue And(BaseValue other)
        {
            if (other is BooleanValue b)
            {
                return new BooleanValue(Value && b.Value);
            }

            throw new WoobyIncompatibleTypesException(this, other);
        }

        public override BaseValue Or(BaseValue other)
        {
            if (other is BooleanValue b)
            {
                return new BooleanValue(Value || b.Value);
            }

            throw new WoobyIncompatibleTypesException(this, other);
        }

        public override int Compare(BaseValue other)
        {
            if (other is BooleanValue b)
            {
                return Value == b.Value ? 0 : -1;
            }
            else if (other is NullValue)
            {
                // FIXME: Remove when IS / IS NOT is implemented
                return -1;
            }
            else throw new WoobyIncompatibleTypesException(this, other);
        }
    }

    public record DateValue : BaseValue
    {
        public DateTime Value { get; private set; }

        public DateValue(DateTime value)
        {
            Value = value;
        }

        public override string PrettyPrint()
        {
            return Value.ToString("u");
        }

        public override BaseValue Add(BaseValue other)
        {
            throw new WoobyIncompatibleTypesException(this, other);
        }

        public override BaseValue Subtract(BaseValue other)
        {
            // Could some day add a if (other is NumberValue)
            throw new WoobyIncompatibleTypesException(this, other);
        }

        public override BaseValue Divide(BaseValue other)
        {
            throw new WoobyIncompatibleTypesException(this, other);
        }

        public override BaseValue Multiply(BaseValue other)
        {
            throw new WoobyIncompatibleTypesException(this, other);
        }

        public override BaseValue Power(BaseValue other)
        {
            throw new WoobyIncompatibleTypesException(this, other);
        }

        public override BaseValue Remainder(BaseValue other)
        {
            throw new WoobyIncompatibleTypesException(this, other);
        }

        public override BaseValue And(BaseValue other)
        {
            throw new WoobyIncompatibleTypesException(this, other);
        }

        public override BaseValue Or(BaseValue other)
        {
            throw new WoobyIncompatibleTypesException(this, other);
        }

        public override int Compare(BaseValue other)
        {
            if (other is DateValue date)
            {
                return Value.CompareTo(date.Value);
            }
            else if (other is NullValue)
            {
                // FIXME: Remove when IS / IS NOT is implemented
                return -1;
            }
            else throw new WoobyIncompatibleTypesException(this, other);
        }
    }

    public struct TempRow
    {
        public Dictionary<string, BaseValue> EvaluatedReferences { get; set; }
        public long RowId;
        public int RowIndex;

        public TempRow()
        {
            EvaluatedReferences = new();
        }
    }

    public enum QueryEvaluationPhase
    {
        Caching,
        Final
    }

    public enum ExpressionOrigin
    {
        OutputColumn,
        Ordering,
        Filter,
        Grouping
    }

    public class EvaluationFlags
    {
        public QueryEvaluationPhase Phase { get; set; }
        public ExpressionOrigin Origin { get; set; }
    }

    public class OutputColumnMeta
    {
        public string OutputName { get; set; }
        public ValueKind Kind { get; set; }
    }

    public class Output
    {
        public List<OutputColumnMeta> Definition { get; set; } = new List<OutputColumnMeta>();
        public List<OutputRow> Rows { get; set; } = new List<OutputRow>();
    }

    // Data for knowing how to order output rows
    public class RowMetaData
    {
        public List<BaseValue> Values { get; set; } = new List<BaseValue>();
        public int RowIndex { get; set; }
    }

    public struct OutputRow
    {
        public List<BaseValue> Values;
        public long RowId;

        public OutputRow()
        {
            Values = new List<BaseValue>();
            RowId = 0;
        }
    }

    public class RowOrderingIntermediate
    {
        public List<long> MatchingRows { get; set; }
        public List<RowOrderingIntermediate> SubOrdering { get; set; }

        public void Collect(ExecutionContext exec, List<OutputRow> target)
        {
            if (MatchingRows != null && MatchingRows.Any())
            {
                target.AddRange(exec.QueryOutput.Rows.Where(r => MatchingRows.Contains(r.RowId)));
            }
            else
            {
                foreach (var sub in SubOrdering)
                {
                    sub.Collect(exec, target);
                }
            }
        }
    }

    public abstract class Function
    {
        public abstract BaseValue WhenCalled(ExecutionContext context, List<BaseValue> arguments, string variantion);
        public abstract IReadOnlyList<FunctionAccepts> Variations { get; }
        public string Name { get; protected set; }
        public long Id { get; protected set; }

        public Function(int Id)
        {
            this.Id = Id;
        }
    }

    public class TableCursor
    {
        private readonly ITableDataProvider Source;
        private readonly int NumCols;
        private long RowId = long.MinValue;
        private IEnumerable<BaseValue> CurrentValues;

        public TableCursor(ITableDataProvider Source, int NumCols)
        {
            this.Source = Source;
            this.NumCols = NumCols;
        }

        public bool Seek(long Id)
        {
            var result = Source.Seek(Id);
            if (result != null)
            {
                RowId = Id;
                CurrentValues = result;
            }

            return result != null;
        }

        public bool SeekNext()
        {
            long id = RowId;
            var result = Source.SeekNext(ref id);
            if (result != null)
            {
                RowId = id;
                CurrentValues = result;
            }

            return result != null;
        }

        public bool SeekFirst()
        {
            return Seek(long.MinValue);
        }

        public long CurrentRowId()
        {
            return RowId;
        }

        public BaseValue Read(int Index)
        {
            if (Index >= NumCols)
            {
                throw new IndexOutOfRangeException("Index for column is out of range");
            }

            return CurrentValues.ElementAt(Index);
        }

        public void Delete()
        {
            RowId = Source.Delete(RowId);
        }

        public void Update(Dictionary<int, BaseValue> values)
        {
            Source.Update(RowId, values);
        }

        public long Insert(Dictionary<int, BaseValue> values)
        {
            var id = Source.Insert(values);
            if (id == long.MinValue || !Seek(id))
            {
                return long.MinValue;
            }

            return id;
        }
    }

    public class TableData
    {
        public TableMeta Meta { get; set; }
        public ITableDataProvider DataProvider { get; set; }
    }

    public class ExecutionDataSource
    {
        public TableMeta Meta { get; set; }
        public TableCursor DataProvider { get; set; }
    }

    public class ExecutionContext
    {
        public Output QueryOutput { get; } = new Output();
        public int RowsAffected { get; set; }
        public Context Context { get; }
        public ExecutionDataSource MainSource { get; set; }
        public Stack<BaseValue> Stack { get; set; } = new Stack<BaseValue>();
        public List<RowMetaData> OrderingResults { get; set; } = new List<RowMetaData>();
        public List<RowMetaData> GroupingResults { get; set; } = new List<RowMetaData>();
        public List<TempRow> TempRows { get; set; } = new List<TempRow>();
        public ExecutionContext Previous { get; set; }
        public int RowNumber { get; private set; }

        public ExecutionContext(Context ctx)
        {
            Context = ctx;
        }

        public void ResetRowNumber()
        {
            RowNumber = 0;
        }

        public void IncrementRowNumber()
        {
            RowNumber += 1;
        }

        public TempRow CreateTempRow()
        {
            return new TempRow {RowId = MainSource.DataProvider.CurrentRowId(), RowIndex = RowNumber};
        }

        public BaseValue PopStack()
        {
            return Stack.Pop();
        }
    }
}