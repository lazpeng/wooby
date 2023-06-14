using System;
using System.Collections.Generic;
using System.Linq;

namespace wooby.Database.Defaults;

public class FunctionAccepts
{
    public string Identifier { get; set; }
    public ColumnType ResultType { get; set; }
    public IReadOnlyList<ColumnType> Parameters { get; set; }
    public bool IsAggregate { get; set; } = false;
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

    public CurrentDate_Function(long Id) : base(Id)
    {
        Name = "CURRENT_DATE";
    }

    public override ColumnValue WhenCalled(ExecutionContext _, List<ColumnValue> __, string variation)
    {
        return new ColumnValue() {Kind = ValueKind.Date, Date = DateTime.Now};
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

    public DatabaseName_Function(long Id) : base(Id)
    {
        Name = "DBNAME";
    }

    public override ColumnValue WhenCalled(ExecutionContext _, List<ColumnValue> __, string variation)
    {
        return new ColumnValue() {Kind = ValueKind.Text, Text = "wooby"};
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

    public RowNum_Function(long Id) : base(Id)
    {
        Name = "ROWNUM";
    }

    public override ColumnValue WhenCalled(ExecutionContext exec, List<ColumnValue> _, string variation)
    {
        return new ColumnValue() {Kind = ValueKind.Number, Number = exec.RowNumber};
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

    public RowId_Function(long Id) : base(Id)
    {
        Name = "ROWID";
    }

    public override ColumnValue WhenCalled(ExecutionContext exec, List<ColumnValue> _, string variation)
    {
        return new ColumnValue() {Kind = ValueKind.Number, Number = exec.MainSource.DataProvider.CurrentRowId()};
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

    public Trunc_Function(long Id) : base(Id)
    {
        Name = "TRUNC";
    }

    public override ColumnValue WhenCalled(ExecutionContext exec, List<ColumnValue> arguments, string variation)
    {
        var date = arguments[0].Date;
        date = date.AddHours(0 - date.Hour).AddMinutes(0 - date.Minute).AddSeconds(0 - date.Second);
        return new ColumnValue() {Kind = ValueKind.Date, Date = date};
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

    public Count_Function(long Id) : base(Id)
    {
        Name = "COUNT";
    }

    public override ColumnValue WhenCalled(ExecutionContext exec, List<ColumnValue> arguments, string variation)
    {
        // Count the number of rows in TempRows
        return new ColumnValue() {Kind = ValueKind.Number, Number = exec.TempRows.Count};
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
    
    public Min_Function(long Id) : base(Id)
    {
        Name = "MIN";
    }

    public override ColumnValue WhenCalled(ExecutionContext context, List<ColumnValue> arguments, string variantion)
    {
        IEnumerable<double> data;
        if (variantion == "regular")
        {
            data = arguments.Select(a => a.Number);
        }
        else
        {
            data = context.TempRows.Select(r => r.EvaluatedReferences[arguments[0].Text].Number);
        }
        var sorted = data.OrderBy(v => v);
        return new ColumnValue() {Kind = ValueKind.Number, Number = sorted.First()};
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
    
    public Max_Function(long Id) : base(Id)
    {
        Name = "MAX";
    }

    public override ColumnValue WhenCalled(ExecutionContext context, List<ColumnValue> arguments, string variantion)
    {
        IEnumerable<double> data;
        if (variantion == "regular")
        {
            data = arguments.Select(a => a.Number);
        }
        else
        {
            data = context.TempRows.Select(r => r.EvaluatedReferences[arguments[0].Text].Number);
        }
        var sorted = data.OrderByDescending(v => v);
        return new ColumnValue() {Kind = ValueKind.Number, Number = sorted.First()};
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

    public Sum_Function(long Id) : base(Id)
    {
        Name = "SUM";
    }

    public override ColumnValue WhenCalled(ExecutionContext context, List<ColumnValue> arguments, string variantion)
    {
        var result = context.TempRows.Select(r => r.EvaluatedReferences[arguments[0].Text].Number).Sum();
        return new ColumnValue() {Kind = ValueKind.Number, Number = result};
    }
}
