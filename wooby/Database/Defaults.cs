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

        public long Delete(long rowid)
        {
            throw new Exception("Not supported");
        }

        public long Insert(Dictionary<int, ColumnValue> values)
        {
            throw new Exception("Not supported");
        }

        public void Update(long rowid, Dictionary<int, ColumnValue> columns)
        {
            throw new Exception("Not supported");
        }

        public IEnumerable<ColumnValue> Seek(long RowId)
        {
            if (RowId == 0)
            {
                return Dummy;
            }
            else return null;
        }

        public IEnumerable<ColumnValue> SeekNext(ref long RowId)
        {
            if (RowId < 0)
            {
                RowId = 0;
                return Seek(0);
            }
            else return null;
        }
    }

    public class InMemory_DataProvider : ITableDataProvider
    {
        private long LastRowId = -1;
        private readonly TableMeta Meta;

        struct Row
        {
            public long RowId;
            public List<ColumnValue> Columns;
        }

        private readonly List<Row> Rows;

        public InMemory_DataProvider(TableMeta Meta)
        {
            Rows = new List<Row>();
            this.Meta = Meta;
        }

        public InMemory_DataProvider(List<List<ColumnValue>> initialValues)
        {
            SetupRows(initialValues);
        }

        protected void SetupRows(List<List<ColumnValue>> Values)
        {
            foreach (var row in Values)
            {
                Rows.Add(new Row { RowId = ++LastRowId, Columns = row });
            }
        }

        private Row? Find(long RowId, out int index)
        {
            for (int i = 0; i < Rows.Count; ++i)
            {
                var row = Rows[i];
                if (row.RowId == RowId || RowId == long.MinValue)
                {
                    index = i;
                    return row;
                }
            }

            index = -1;

            return null;
        }

        IEnumerable<ColumnValue> ITableDataProvider.Seek(long RowId)
        {
            return Find(RowId, out int _)?.Columns;
        }

        public IEnumerable<ColumnValue> SeekNext(ref long RowId)
        {
            Find(RowId, out int index);
            
            if (Rows.Count > index + 1)
            {
                var row = Rows[index + 1];
                RowId = row.RowId;
                return row.Columns;
            }

            RowId = long.MinValue;
            return null;
        }

        public long Delete(long rowid)
        {
            Find(rowid, out int index);
            Rows.RemoveAt(index);
            if (index == 0 || index >= Rows.Count)
            {
                return long.MinValue;
            }
            else
            {
                return Rows[index - 1].RowId;
            }
        }

        public long Insert(Dictionary<int, ColumnValue> values)
        {
            var row = new Row { RowId = ++LastRowId, Columns = new List<ColumnValue>(Meta.Columns.Count) };

            for (int idx = 0; idx < Meta.Columns.Count; ++idx)
            {
                if (values.TryGetValue(idx, out ColumnValue v))
                {
                    row.Columns.Add(v);
                } else
                {
                    row.Columns.Add(new ColumnValue { Kind = ValueKind.Null });
                }
            }

            Rows.Add(row);

            return row.RowId;
        }

        public void Update(long rowid, Dictionary<int, ColumnValue> columns)
        {
            var row = Find(rowid, out int _);

            if (row != null)
            {
                foreach (var col in columns)
                {
                    row.Value.Columns[col.Key] = col.Value;
                }
            }
        }
    }

    public class LoveLive_DataProvider : InMemory_DataProvider
    {
        private class Group
        {
            public long Id;
            public long? ParentId = null;
            public string Nome;
            public int Ano;
            public int NumIntegrantes;
        }

        private static readonly List<Group> Groups = new()
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

        public LoveLive_DataProvider(TableMeta Meta) : base(Meta)
        {
            SetupRows(Groups.Select(g =>
                new List<ColumnValue>()
                {
                    new ColumnValue() { Kind = ValueKind.Number, Number = g.Id },
                    g.ParentId != null ? new ColumnValue { Kind = ValueKind.Number, Number = g.ParentId.Value } : new ColumnValue { Kind = ValueKind.Null },
                    new ColumnValue() { Kind = ValueKind.Text, Text = g.Nome },
                    new ColumnValue() { Kind = ValueKind.Number, Number = g.Ano },
                    new ColumnValue() { Kind = ValueKind.Number, Number = g.NumIntegrantes }
                }).ToList());
        }
    }
}
