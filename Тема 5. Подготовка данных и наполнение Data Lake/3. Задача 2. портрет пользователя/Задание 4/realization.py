def compare_df(df1, df2):

    df_columns = df1.columns == df2.columns
    print('Колонки =', df_columns)

    df_counts = df1.dropDuplicates().count() == df2.dropDuplicates().count() == df1.union(df2).dropDuplicates().count()

    print('Количество строк df1 = ', df1.dropDuplicates().count())
    print('Количество строк df2 = ', df2.dropDuplicates().count())
    print('Количество строк union = ', df1.union(df2).dropDuplicates().count())

    print('Количество строк проверка', df_counts)

    test = [df_columns, df_counts]

    if all(test):
        print('true')
    else:
        print('false')