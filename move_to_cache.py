import sys
from column_cacher import ColumnCacher


def main(table_name, column_name):

    with ColumnCacher(table_name, column_name) as cc:
        cc.touch_column()

if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])