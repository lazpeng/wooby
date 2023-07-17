using System;
using System.Collections.Generic;
using System.Linq;
using wooby.Error;

namespace wooby.Database.Defaults;

public class FunctionAccepts
{
    public string Identifier { get; init; }
    public ColumnType ResultType { get; init; }
    public IReadOnlyList<ColumnType> Parameters { get; init; }
    public bool IsAggregate { get; init; }
}

internal class CurrentDateFunction : Function
{
    public override IReadOnlyList<FunctionAccepts> Variations { get; } = new[]
    {
        new FunctionAccepts
        {
            Identifier = string.Empty,
            Parameters = Array.Empty<ColumnType>(),
            ResultType = ColumnType.Date
        }
    };

    public CurrentDateFunction(int id) : base(id)
    {
        Name = "CURRENT_DATE";
    }

    public override BaseValue WhenCalled(ExecutionContext _, List<BaseValue> __, string variation)
    {
        return new DateValue(DateTime.Now);
    }
}

internal class DatabaseNameFunction : Function
{
    public override IReadOnlyList<FunctionAccepts> Variations { get; } = new[]
    {
        new FunctionAccepts
        {
            Identifier = string.Empty,
            Parameters = Array.Empty<ColumnType>(),
            ResultType = ColumnType.String
        }
    };

    public DatabaseNameFunction(int id) : base(id)
    {
        Name = "DBNAME";
    }

    public override BaseValue WhenCalled(ExecutionContext _, List<BaseValue> __, string variation)
    {
        return new TextValue("Wooby");
    }
}

internal class RowNumFunction : Function
{
    public override IReadOnlyList<FunctionAccepts> Variations { get; } = new[]
    {
        new FunctionAccepts
        {
            Identifier = string.Empty,
            Parameters = Array.Empty<ColumnType>(),
            ResultType = ColumnType.Number
        }
    };

    public RowNumFunction(int id) : base(id)
    {
        Name = "ROWNUM";
    }

    public override BaseValue WhenCalled(ExecutionContext exec, List<BaseValue> _, string variation)
    {
        return new NumberValue(exec.RowNumber);
    }
}

internal class RowIdFunction : Function
{
    public override IReadOnlyList<FunctionAccepts> Variations { get; } = new[]
    {
        new FunctionAccepts
        {
            Identifier = string.Empty,
            Parameters = Array.Empty<ColumnType>(),
            ResultType = ColumnType.Number
        }
    };

    public RowIdFunction(int id) : base(id)
    {
        Name = "ROWID";
    }

    public override BaseValue WhenCalled(ExecutionContext exec, List<BaseValue> _, string variation)
    {
        return new NumberValue(exec.MainSource.DataProvider.CurrentRowId());
    }
}

internal class TruncFunction : Function
{
    public override IReadOnlyList<FunctionAccepts> Variations { get; } = new[]
    {
        new FunctionAccepts
        {
            Identifier = string.Empty,
            Parameters = new[] { ColumnType.Date },
            ResultType = ColumnType.Date
        }
    };

    public TruncFunction(int id) : base(id)
    {
        Name = "TRUNC";
    }

    public override BaseValue WhenCalled(ExecutionContext exec, List<BaseValue> arguments, string variation)
    {
        if (arguments[0] is not DateValue d) return new NullValue();

        var date = d.Value;
        date = date.AddHours(0 - date.Hour).AddMinutes(0 - date.Minute).AddSeconds(0 - date.Second);
        return new DateValue(date);
    }
}

internal class CountFunction : Function
{
    public override IReadOnlyList<FunctionAccepts> Variations { get; } = new[]
    {
        new FunctionAccepts
        {
            Identifier = "agg",
            IsAggregate = true,
            Parameters = Array.Empty<ColumnType>(),
            ResultType = ColumnType.Number
        }
    };

    public CountFunction(int id) : base(id)
    {
        Name = "COUNT";
    }

    public override BaseValue WhenCalled(ExecutionContext exec, List<BaseValue> arguments, string variation)
    {
        // Count the number of rows in TempRows
        return new NumberValue(exec.TempRows.Count);
    }
}

internal class MinFunction : Function
{
    public override IReadOnlyList<FunctionAccepts> Variations { get; } = new[]
    {
        new FunctionAccepts
        {
            Identifier = "regular",
            IsAggregate = false,
            Parameters = new[] { ColumnType.Number, ColumnType.Number },
            ResultType = ColumnType.Number
        },
        new FunctionAccepts
        {
            Identifier = "agg",
            IsAggregate = true,
            Parameters = new[] { ColumnType.Number },
            ResultType = ColumnType.Number
        }
    };

    public MinFunction(int id) : base(id)
    {
        Name = "MIN";
    }

    public override BaseValue WhenCalled(ExecutionContext context, List<BaseValue> arguments, string variation)
    {
        IEnumerable<double> data;
        if (variation == "regular")
        {
            data = arguments.Select(a => ((NumberValue)a).Value);
        }
        else
        {
            if (arguments[0] is TextValue eval)
            {
                data = context.TempRows.Select(r => ((NumberValue)r.EvaluatedReferences[eval.Value]).Value);
            }
            else
                throw new WoobyDatabaseException(
                    "Internal error: Expected evaluated reference name for aggregate function call");
        }

        var sorted = data.OrderBy(v => v);
        return new NumberValue(sorted.First());
    }
}

internal class MaxFunction : Function
{
    public override IReadOnlyList<FunctionAccepts> Variations { get; } = new[]
    {
        new FunctionAccepts
        {
            Identifier = "regular",
            IsAggregate = false,
            Parameters = new[] { ColumnType.Number, ColumnType.Number },
            ResultType = ColumnType.Number
        },
        new FunctionAccepts
        {
            Identifier = "agg",
            IsAggregate = true,
            Parameters = new[] { ColumnType.Number },
            ResultType = ColumnType.Number
        }
    };

    public MaxFunction(int id) : base(id)
    {
        Name = "MAX";
    }

    public override BaseValue WhenCalled(ExecutionContext context, List<BaseValue> arguments, string variation)
    {
        // FIXME
        IEnumerable<double> data;
        if (variation == "regular")
        {
            data = arguments.Select(a => ((NumberValue)a).Value);
        }
        else
        {
            if (arguments[0] is TextValue eval)
            {
                data = context.TempRows.Select(r => ((NumberValue)r.EvaluatedReferences[eval.Value]).Value);
            }
            else
                throw new WoobyDatabaseException(
                    "Internal error: Expected evaluated reference name for aggregate function call");
        }

        var sorted = data.OrderByDescending(v => v);
        return new NumberValue(sorted.First());
    }
}

internal class SumFunction : Function
{
    public override IReadOnlyList<FunctionAccepts> Variations { get; } = new[]
    {
        new FunctionAccepts
        {
            Identifier = "",
            IsAggregate = true,
            Parameters = new[] { ColumnType.Number },
            ResultType = ColumnType.Number
        }
    };

    public SumFunction(int id) : base(id)
    {
        Name = "SUM";
    }

    public override BaseValue WhenCalled(ExecutionContext context, List<BaseValue> arguments, string variation)
    {
        // FIXME
        if (arguments[0] is not TextValue eval)
        {
            throw new WoobyDatabaseException(
                "Internal error: Expected evaluated reference name for aggregate function call");
        }

        var result = context.TempRows.Select(r => ((NumberValue)r.EvaluatedReferences[eval.Value]).Value).Sum();
        return new NumberValue(result);
    }
}