using System;
using System.Collections.Generic;
using System.Linq;
using wooby.Database.Defaults;
using wooby.Database.Persistence;
using wooby.Error;

namespace wooby.Database;

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

public record TextValue(string Value) : BaseValue
{
    public string Value { get; } = Value;

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
        return other switch
        {
            // Does not compare if greater or lesser, only equals or not
            TextValue otherText => Value == otherText.Value ? 0 : -1,
            NullValue => -1,
            _ => throw new WoobyIncompatibleTypesException(this, other)
        };
    }
}

public record NumberValue(double Value) : BaseValue
{
    public double Value { get; } = Value;

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

        throw new WoobyIncompatibleTypesException(this, other);
    }

    public override BaseValue Subtract(BaseValue other)
    {
        if (other is NumberValue num)
        {
            return new NumberValue(Value - num.Value);
        }

        throw new WoobyIncompatibleTypesException(this, other);
    }

    public override BaseValue Divide(BaseValue other)
    {
        if (other is NumberValue num)
        {
            return new NumberValue(Value / num.Value);
        }

        throw new WoobyIncompatibleTypesException(this, other);
    }

    public override BaseValue Multiply(BaseValue other)
    {
        if (other is NumberValue num)
        {
            return new NumberValue(Value * num.Value);
        }

        throw new WoobyIncompatibleTypesException(this, other);
    }

    public override BaseValue Power(BaseValue other)
    {
        if (other is NumberValue num)
        {
            return new NumberValue(Math.Pow(Value, num.Value));
        }

        throw new WoobyIncompatibleTypesException(this, other);
    }

    public override BaseValue Remainder(BaseValue other)
    {
        if (other is NumberValue num)
        {
            return new NumberValue(Value % num.Value);
        }

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
        return other switch
        {
            NumberValue num when Value < num.Value => -1,
            NumberValue num => Value > num.Value ? 1 : 0,
            NullValue => -1,
            _ => throw new WoobyIncompatibleTypesException(this, other)
        };
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

public record BooleanValue(bool Value) : NullValue
{
    public override string PrettyPrint()
    {
        return Value ? "TRUE" : "FALSE";
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
        return other switch
        {
            BooleanValue b => Value == b.Value ? 0 : -1,
            NullValue => -1,
            _ => throw new WoobyIncompatibleTypesException(this, other)
        };
    }
}

public record DateValue(DateTime Value) : BaseValue
{
    public DateTime Value { get; } = Value;

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
        return other switch
        {
            DateValue date => Value.CompareTo(date.Value),
            NullValue => -1,
            _ => throw new WoobyIncompatibleTypesException(this, other)
        };
    }
}

public struct TempRow
{
    public Dictionary<string, BaseValue> EvaluatedReferences { get; }
    public long RowId;

    public TempRow()
    {
        EvaluatedReferences = new Dictionary<string, BaseValue>();
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

public record EvaluationFlags(QueryEvaluationPhase Phase, ExpressionOrigin Origin)
{
    public QueryEvaluationPhase Phase { get; } = Phase;
    public ExpressionOrigin Origin { get; set; } = Origin;
}

public class OutputColumnMeta
{
    public string OutputName { get; init; } = string.Empty;
    public ValueKind Kind { get; set; }
}

public class Output
{
    public List<OutputColumnMeta> Definition { get; } = new();
    public List<OutputRow> Rows { get; set; } = new();
}

public struct OutputRow
{
    public readonly List<BaseValue> Values;
    public long RowId;

    public OutputRow()
    {
        Values = new List<BaseValue>();
        RowId = 0;
    }
}

public class RowOrderingIntermediate
{
    public List<long>? MatchingRows { get; set; }
    public List<RowOrderingIntermediate> SubOrdering { get; set; } = new();

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
    public abstract BaseValue WhenCalled(ExecutionContext context, List<BaseValue> arguments, string variation);
    public abstract IEnumerable<FunctionAccepts> Variations { get; }
    public string Name { get; protected init; } = string.Empty;
    public long Id { get; protected set; }

    protected Function(int id)
    {
        Id = id;
    }
}

public class TableCursor
{
    private readonly ITableDataProvider _source;
    private long _rowId = long.MinValue;
    private IEnumerable<BaseValue>? _currentValues;

    public TableCursor(ITableDataProvider source)
    {
        _source = source;
    }

    public bool Seek(long id)
    {
        var result = _source.Seek(id);
        if (result != null)
        {
            _rowId = id;
            _currentValues = result;
        }

        return result != null;
    }

    public bool SeekNext()
    {
        var id = _rowId;
        var result = _source.SeekNext(ref id);
        
        if (result != null)
        {
            _rowId = id;
            _currentValues = result;
        }

        return result != null;
    }

    public void SeekFirst()
    {
        Seek(long.MinValue);
    }

    public long CurrentRowId()
    {
        return _rowId;
    }

    public BaseValue Read(int index)
    {
        // TODO: Check if should throw error
        return _currentValues?.ElementAt(index) ?? new NullValue();
    }

    public void Delete()
    {
        _rowId = _source.Delete(_rowId);
    }

    public void Update(Dictionary<int, BaseValue> values)
    {
        _source.Update(_rowId, values);
    }

    public long Insert(Dictionary<int, BaseValue> values)
    {
        var id = _source.Insert(values);
        if (id == long.MinValue || !Seek(id))
        {
            return long.MinValue;
        }

        return id;
    }
}

public class ExecutionDataSource
{
    public TableMeta Meta { get; }
    public TableCursor DataProvider { get; }
    private string Alias { get; }
    public bool LastMatched { get; set; }
    public bool Matched { get; set; }

    public ExecutionDataSource(TableMeta meta, TableCursor dataProvider, string alias)
    {
        Meta = meta;
        DataProvider = dataProvider;
        Alias = alias;
    }

    public bool NameMatches(string name)
    {
        return (string.IsNullOrEmpty(Alias) ? Meta.Name : Alias) == name;
    }
}

public class ExecutionContext
{
    public Output QueryOutput { get; } = new();
    public int RowsAffected { get; set; }
    public Context Context { get; }
    public ExecutionDataSource MainSource => Sources.First();
    public List<ExecutionDataSource> Sources { get; } = new();
    public Stack<BaseValue> Stack { get; } = new();
    public List<TempRow> TempRows { get; set; } = new();
    public ExecutionContext? Previous { get; init; }
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
        return new TempRow { RowId = MainSource.DataProvider.CurrentRowId() };
    }

    public BaseValue PopStack()
    {
        return Stack.Pop();
    }
}