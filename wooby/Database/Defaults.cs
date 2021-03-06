using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace wooby.Database
{
    class CurrentDate_Function : Function
    {
        public CurrentDate_Function(long Id) : base(Id)
        {
            Name = "CURRENT_DATE";
            ResultType = ColumnType.Date;
            Parameters = new List<ColumnType>();
        }

        public override ColumnValue WhenCalled(ExecutionContext _, List<ColumnValue> __)
        {
            return new ColumnValue() { Kind = ValueKind.Date, Date = DateTime.Now };
        }
    }

    class DatabaseName_Function : Function
    {
        public DatabaseName_Function(long Id) : base(Id)
        {
            Name = "DBNAME";
            ResultType = ColumnType.String;
            Parameters = new List<ColumnType>();
        }

        public override ColumnValue WhenCalled(ExecutionContext _, List<ColumnValue> __)
        {
            return new ColumnValue() { Kind = ValueKind.Text, Text = "wooby" };
        }
    }

    class RowNum_Function : Function
    {
        public RowNum_Function(long Id) : base(Id)
        {
            Name = "ROWNUM";
            ResultType = ColumnType.Number;
            Parameters = new List<ColumnType>();
        }

        public override ColumnValue WhenCalled(ExecutionContext exec, List<ColumnValue> _)
        {
            return new ColumnValue() { Kind = ValueKind.Number, Number = exec.QueryOutput.Rows.Count };
        }
    }

    class RowId_Function : Function
    {
        public RowId_Function(long Id) : base(Id)
        {
            Name = "ROWID";
            ResultType = ColumnType.Number;
            Parameters = new List<ColumnType>();
        }

        public override ColumnValue WhenCalled(ExecutionContext exec, List<ColumnValue> _)
        {
            return new ColumnValue() { Kind = ValueKind.Number, Number = exec.MainSource.DataProvider.CurrentRowId() };
        }
    }

    class Trunc_Function : Function
    {
        public Trunc_Function(long Id) : base(Id)
        {
            Name = "TRUNC";
            ResultType = ColumnType.Date;
            Parameters = new List<ColumnType>() { ColumnType.Date };
        }

        public override ColumnValue WhenCalled(ExecutionContext exec, List<ColumnValue> arguments)
        {
            var date = arguments[0].Date;
            date = date.AddHours(0 - date.Hour).AddMinutes(0 - date.Minute).AddSeconds(0 - date.Second);
            return new ColumnValue() { Kind = ValueKind.Date, Date = date };
        }
    }

    public class Dual_DataProvider : ITableDataProvider
    {
        private readonly List<ColumnValue> Dummy = new();

        public IEnumerable<ColumnValue> Seek(long RowId)
        {
            if (RowId == 0)
            {
                return Dummy;
            } else return null;
        }

        public IEnumerable<ColumnValue> SeekNext(ref long RowId)
        {
            if (RowId < 0)
            {
                RowId = 0;
                return Seek(0);
            } else return null;
        }
    }

    public class Stub_DataProvider : ITableDataProvider
    {
        public IEnumerable<ColumnValue> Seek(long RowId)
        {
            return null;
        }

        public IEnumerable<ColumnValue> SeekNext(ref long RowId)
        {
            return null;
        }
    }

    public class LoveLive_DataProvider : ITableDataProvider
    {
        private class Group
        {
            public long Id;
            public long? ParentId = null;
            public string Nome;
            public int Ano;
            public int NumIntegrantes;
        }

        private readonly List<Group> groups = new()
        {
            new Group() { Id = 0, Nome = "μ's", Ano = 2010, NumIntegrantes = 9 },
            new Group() { Id = 1, Nome = "Aqours", Ano = 2016, NumIntegrantes = 9 },
            new Group() { Id = 2, Nome = "Nijigasaki School Idol Club", Ano = 2017, NumIntegrantes = 10 },
            new Group() { Id = 3, Nome = "Liella", Ano = 2020, NumIntegrantes = 5 },
            new Group() { Id = 4, Nome = "BiBi", Ano = 2011, NumIntegrantes = 3, ParentId = 0 },
            new Group() { Id = 5, Nome = "Lily white", Ano = 2011, NumIntegrantes = 3, ParentId = 0 },
            new Group() { Id = 6, Nome = "Printemps", Ano = 2011, NumIntegrantes = 3, ParentId = 0 },
            new Group() { Id = 7, Nome = "Guilty kiss", Ano = 2016, NumIntegrantes = 3, ParentId = 1 },
            new Group() { Id = 8, Nome = "CYaRon", Ano = 2016, NumIntegrantes = 3, ParentId = 1 },
            new Group() { Id = 9, Nome = "AZALEA", Ano = 2016, NumIntegrantes = 3, ParentId = 1 },
        };

        private readonly List<List<ColumnValue>> Rows;

        public LoveLive_DataProvider()
        {
            Rows = groups.Select(g =>
                new List<ColumnValue>()
                {
                    new ColumnValue() { Kind = ValueKind.Number, Number = g.Id },
                    g.ParentId != null ? new ColumnValue { Kind = ValueKind.Number, Number = g.ParentId.Value } : new ColumnValue { Kind = ValueKind.Null },
                    new ColumnValue() { Kind = ValueKind.Text, Text = g.Nome },
                    new ColumnValue() { Kind = ValueKind.Number, Number = g.Ano },
                    new ColumnValue() { Kind = ValueKind.Number, Number = g.NumIntegrantes }
                }).ToList();
        }

        IEnumerable<ColumnValue> ITableDataProvider.Seek(long RowId)
        {
            if (RowId >= Rows.Count)
            {
                return null;
            }

            return Rows[(int) RowId];
        }

        public IEnumerable<ColumnValue> SeekNext(ref long RowId)
        {
            if (RowId + 1 >= Rows.Count)
            {
                return null;
            }

            RowId += 1;

            return Rows[(int) RowId];
        }
    }
}
