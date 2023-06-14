using System;
using System.Collections.Generic;
using System.Linq;

namespace wooby.Database.Defaults;

public class FunctionAccepts
{
    public string Identifier { get; set; }
    public ColumnType ResultType { get; set; }
    public IReadOnlyList<ColumnType> Parameters { get; set; }
    public bool IsAggregate { get; set; }
}

class CurrentDate_Function : Function
{
    public override IReadOnlyList<FunctionAccepts> Variations { get; } = new[]
    {
        new FunctionAccepts
        {
            Identifier = String.Empty,
            Parameters = new ColumnType[] { },
            ResultType = ColumnType.Date
        }
    };

    public CurrentDate_Function(int Id) : base(Id)
    {
        Name = "CURRENT_DATE";
    }

    public override BaseValue WhenCalled(ExecutionContext _, List<BaseValue> __, string variation)
    {
        return new DateValue(DateTime.Now);
    }
}

class DatabaseName_Function : Function
{
    public override IReadOnlyList<FunctionAccepts> Variations { get; } = new[]
    {
        new FunctionAccepts
        {
            Identifier = String.Empty,
            Parameters = new ColumnType[] { },
            ResultType = ColumnType.String
        }
    };

    public DatabaseName_Function(int Id) : base(Id)
    {
        Name = "DBNAME";
    }

    public override BaseValue WhenCalled(ExecutionContext _, List<BaseValue> __, string variation)
    {
        return new TextValue("Wooby");
    }
}

class RowNum_Function : Function
{
    public override IReadOnlyList<FunctionAccepts> Variations { get; } = new[]
    {
        new FunctionAccepts
        {
            Identifier = String.Empty,
            Parameters = new ColumnType[] { },
            ResultType = ColumnType.Number
        }
    };

    public RowNum_Function(int Id) : base(Id)
    {
        Name = "ROWNUM";
    }

    public override BaseValue WhenCalled(ExecutionContext exec, List<BaseValue> _, string variation)
    {
        return new NumberValue(exec.RowNumber);
    }
}

class RowId_Function : Function
{
    public override IReadOnlyList<FunctionAccepts> Variations { get; } = new[]
    {
        new FunctionAccepts
        {
            Identifier = String.Empty,
            Parameters = new ColumnType[] { },
            ResultType = ColumnType.Number
        }
    };

    public RowId_Function(int Id) : base(Id)
    {
        Name = "ROWID";
    }

    public override BaseValue WhenCalled(ExecutionContext exec, List<BaseValue> _, string variation)
    {
        return new NumberValue(exec.MainSource.DataProvider.CurrentRowId());
    }
}

class Trunc_Function : Function
{
    public override IReadOnlyList<FunctionAccepts> Variations { get; } = new[]
    {
        new FunctionAccepts()
        {
            Identifier = String.Empty,
            Parameters = new[] {ColumnType.Date},
            ResultType = ColumnType.Date
        }
    };

    public Trunc_Function(int Id) : base(Id)
    {
        Name = "TRUNC";
    }

    public override BaseValue WhenCalled(ExecutionContext exec, List<BaseValue> arguments, string variation)
    {
        if (arguments[0] is DateValue d)
        {
            var date = d.Value;
            date = date.AddHours(0 - date.Hour).AddMinutes(0 - date.Minute).AddSeconds(0 - date.Second);
            return new DateValue(date);
        }

        return new NullValue();
    }
}

class Count_Function : Function
{
    public override IReadOnlyList<FunctionAccepts> Variations { get; } = new[]
    {
        new FunctionAccepts()
        {
            Identifier = "agg",
            IsAggregate = true,
            Parameters = new ColumnType[] { },
            ResultType = ColumnType.Number
        }
    };

    public Count_Function(int Id) : base(Id)
    {
        Name = "COUNT";
    }

    public override BaseValue WhenCalled(ExecutionContext exec, List<BaseValue> arguments, string variation)
    {
        // Count the number of rows in TempRows
        return new NumberValue(exec.TempRows.Count);
    }
}

class Min_Function : Function
{
    
    public override IReadOnlyList<FunctionAccepts> Variations { get; } = new[]
    {
        new FunctionAccepts()
        {
            Identifier = "regular",
            IsAggregate = false,
            Parameters = new[] {ColumnType.Number, ColumnType.Number},
            ResultType = ColumnType.Number
        },
        new FunctionAccepts()
        {
            Identifier = "agg",
            IsAggregate = true,
            Parameters = new[] {ColumnType.Number},
            ResultType = ColumnType.Number
        }
    };
    
    public Min_Function(int Id) : base(Id)
    {
        Name = "MIN";
    }

    public override BaseValue WhenCalled(ExecutionContext context, List<BaseValue> arguments, string variantion)
    {
        // FIXME: Terrible
        IEnumerable<double> data;
        if (variantion == "regular")
        {
            data = arguments.Select(a => (a as NumberValue).Value);
        }
        else
        {
            data = context.TempRows.Select(r => (r.EvaluatedReferences[(arguments[0] as TextValue).Value] as NumberValue).Value);
        }
        var sorted = data.OrderBy(v => v);
        return new NumberValue(sorted.First());
    }
}

class Max_Function : Function
{
    public override IReadOnlyList<FunctionAccepts> Variations { get; } = new[]
    {
        new FunctionAccepts()
        {
            Identifier = "regular",
            IsAggregate = false,
            Parameters = new[] {ColumnType.Number, ColumnType.Number},
            ResultType = ColumnType.Number
        },
        new FunctionAccepts()
        {
            Identifier = "agg",
            IsAggregate = true,
            Parameters = new[] {ColumnType.Number},
            ResultType = ColumnType.Number
        }
    };
    
    public Max_Function(int Id) : base(Id)
    {
        Name = "MAX";
    }

    public override BaseValue WhenCalled(ExecutionContext context, List<BaseValue> arguments, string variantion)
    {
        // FIXME
        IEnumerable<double> data;
        if (variantion == "regular")
        {
            data = arguments.Select(a => (a as NumberValue).Value);
        }
        else
        {
            data = context.TempRows.Select(r => (r.EvaluatedReferences[(arguments[0] as TextValue).Value] as NumberValue).Value);
        }
        var sorted = data.OrderByDescending(v => v);
        return new NumberValue(sorted.First());
    }
}

class Sum_Function : Function
{
    public override IReadOnlyList<FunctionAccepts> Variations { get; } = new[]
    {
        new FunctionAccepts()
        {
            Identifier = "",
            IsAggregate = true,
            Parameters = new[] {ColumnType.Number},
            ResultType = ColumnType.Number
        }
    };

    public Sum_Function(int Id) : base(Id)
    {
        Name = "SUM";
    }

    public override BaseValue WhenCalled(ExecutionContext context, List<BaseValue> arguments, string variantion)
    {
        // FIXME
        var result = context.TempRows.Select(r => (r.EvaluatedReferences[(arguments[0] as TextValue).Value] as NumberValue).Value).Sum();
        return new NumberValue(result);
    }
}
