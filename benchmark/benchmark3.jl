

dir = "/Users/jacobquinn/csv-comparison/data/"

for d in (
    "rows_100_cols_20_na_false",
    "rows_100_cols_20_na_true",
    "rows_100_cols_200_na_false",
    "rows_100_cols_200_na_true",
    "rows_10000_cols_20_na_false",
    "rows_10000_cols_20_na_true",
    "rows_10000_cols_200_na_false",
    "rows_10000_cols_200_na_true",
    "rows_1000000_cols_20_na_false",
    "rows_1000000_cols_20_na_true",
    "rows_1000000_cols_200_na_false",
    "rows_1000000_cols_200_na_true",
), f in (
    "mixed_data.csv",
    "uniform_data_catstring.csv",
    "uniform_data_float64.csv",
    "uniform_data_shortfloat64.csv",
    "mixed_data_shortfloat64.csv",
    "uniform_data_datetime.csv",
    "uniform_data_int64.csv",
    "uniform_data_string.csv",
)
    file = joinpath(dir, d, f)
    println("file=$file")
    GC.enable(false)
    @time CSV.read(file)
    GC.enable(true)
    GC.gc(); GC.gc()
    GC.enable(false)
    @time TextParse.csvread(file)
    GC.enable(true)
    GC.gc(); GC.gc()
end


file = "/Users/jacobquinn/csv-comparison/data/rows_1000000_cols_20_na_false/uniform_data_int64.csv"
@time CSV.read(file)