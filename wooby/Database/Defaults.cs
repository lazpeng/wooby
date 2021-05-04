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
            return new ColumnValue() { Kind = ValueKind.Number, Number = exec.MainSource.DataProvider.RowId() };
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

    class SayHello_Function : Function
    {
        public SayHello_Function(long Id) : base(Id)
        {
            Name = "DIZ_OI";
            ResultType = ColumnType.String;
            Parameters = new List<ColumnType>() { ColumnType.String };
        }

        public override ColumnValue WhenCalled(ExecutionContext exec, List<ColumnValue> arguments)
        {
            var nome = arguments[0].Text;
            return new ColumnValue() { Kind = ValueKind.Text, Text = $"Oi, {nome}!" };
        }
    }

    public class Dual_DataProvider : ITableDataProvider
    {
        private long index = 0;
        public ColumnValue GetColumn(int index)
        {
            return null;
        }

        public void Reset()
        {
            // Stub
        }

        public long RowId()
        {
            return index;
        }

        public bool Seek(long RowId)
        {
            return RowId == 0;
        }

        public bool SeekNext()
        {
            return index++ <= 0;
        }

        public List<ColumnValue> WholeRow()
        {
            return null;
        }
    }

    public class LoveLive_DataProvider : ITableDataProvider
    {
        private class Group
        {
            public string Nome;
            public int Ano;
            public int NumIntegrantes;
        }

        private List<Group> grupos = new()
        {
            new Group() { Nome = "μ's", Ano = 2010, NumIntegrantes = 9 },
            new Group() { Nome = "Aqours", Ano = 2016, NumIntegrantes = 9 },
            new Group() { Nome = "Nijigasaki School Idol Club", Ano = 2017, NumIntegrantes = 10 },
            new Group() { Nome = "Liella", Ano = 2020, NumIntegrantes = 5 },
        };

        private int cursor = -1;

        public ColumnValue GetColumn(int index)
        {
            if (cursor < 0)
            {
                throw new InvalidOperationException();
            }

            var grupo = grupos[cursor];

            return index switch
            {
                0 => new ColumnValue() { Kind = ValueKind.Text, Text = grupo.Nome },
                1 => new ColumnValue() { Kind = ValueKind.Number, Number = grupo.Ano },
                2 => new ColumnValue() { Kind = ValueKind.Number, Number = grupo.NumIntegrantes },
                _ => throw new InvalidOperationException()
            };
        }

        public void Reset()
        {
            cursor = -1;
        }

        public long RowId()
        {
            return cursor;
        }

        public bool Seek(long RowId)
        {
            if (RowId < grupos.Count)
            {
                cursor = (int)RowId;
                return true;
            }
            else return false;
        }

        public bool SeekNext()
        {
            return ++cursor < grupos.Count;
        }

        public List<ColumnValue> WholeRow()
        {
            if (cursor < 0)
            {
                throw new InvalidOperationException();
            }

            var grupo = grupos[cursor];

            return new()
            {
                new ColumnValue() { Kind = ValueKind.Text, Text = grupo.Nome },
                new ColumnValue() { Kind = ValueKind.Number, Number = grupo.Ano },
                new ColumnValue() { Kind = ValueKind.Number, Number = grupo.NumIntegrantes }
            };
        }
    }
}
