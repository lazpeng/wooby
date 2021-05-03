using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace wooby.Database
{
    class CurrentDate_Variable : DynamicVariable
    {
        public CurrentDate_Variable(long Id) : base(Id)
        {
            Name = "CURRENT_DATE";
            ResultType = ColumnType.String;
        }

        public override ColumnValue WhenCalled(ExecutionContext _)
        {
            var currentDate = DateTime.Now.ToString("u");
            return new ColumnValue() { Kind = ValueKind.Text, Text = currentDate };
        }
    }

    class DatabaseName_Variable : DynamicVariable
    {
        public DatabaseName_Variable(long Id) : base(Id)
        {
            Name = "DBNAME";
            ResultType = ColumnType.String;
        }

        public override ColumnValue WhenCalled(ExecutionContext _)
        {
            return new ColumnValue() { Kind = ValueKind.Text, Text = "wooby" };
        }
    }

    class RowNum_Variable : DynamicVariable
    {
        public RowNum_Variable(long Id) : base(Id)
        {
            Name = "ROWNUM";
            ResultType = ColumnType.Number;
        }

        public override ColumnValue WhenCalled(ExecutionContext exec)
        {
            return new ColumnValue() { Kind = ValueKind.Text, Number = exec.QueryOutput.Rows.Count };
        }
    }

    public class Dual_DataProvider : ITableDataProvider
    {
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
            return -1;
        }

        public bool Seek(long RowId)
        {
            return false;
        }

        public bool SeekNext()
        {
            return false;
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
