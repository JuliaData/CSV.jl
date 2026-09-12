using CSV, Tables, Test

# A string extension records the tasks that perform output conversion. Yielding
# inside the conversion exposes overlapping jobs even on a single CPU thread.
struct TaskTestString <: AbstractString
    value::String
end
Base.String(s::TaskTestString) = s.value
CSV._stringsink(::Type{TaskTestString}) = true
const CONVERSION_LOCK = ReentrantLock()
const CONVERSION_ACTIVE = Ref(0)
const CONVERSION_PEAK = Ref(0)
const CONVERSION_TASKS = Set{Task}()
function CSV._materializecolumn(::Type{TaskTestString}, col::CSV.DataStringVector)
    lock(CONVERSION_LOCK) do
        CONVERSION_ACTIVE[] += 1
        CONVERSION_PEAK[] = max(CONVERSION_PEAK[], CONVERSION_ACTIVE[])
        push!(CONVERSION_TASKS, current_task())
    end
    try
        sleep(0.01)
        return TaskTestString.(CSV.materialize(col))
    finally
        lock(CONVERSION_LOCK) do
            CONVERSION_ACTIVE[] -= 1
        end
    end
end

@testset "reader task budget includes string conversion" begin
    ncols = 8
    names = ["c$j" for j in 1:ncols]
    input = Vector{UInt8}(join(names, ',') * "\n" * (join(fill("text", ncols), ',') * "\n")^20)
    transposed = Vector{UInt8}(join((name * "," * join(fill("text", 20), ',') for name in names), '\n'))
    lazy = CSV.lazy(input)
    for ntasks in (1, 2), parallel in (false, true)
        budget = parallel ? min(ntasks, Threads.nthreads()) : 1
        options = (; ntasks, parallel, stringtype=TaskTestString, pool=false)
        readers = (
            () -> CSV.File(input; options...),
            () -> CSV.File(lazy; options...),
            () -> CSV.File(input; scan=Tables.Scan(), options...),
            () -> CSV.File(transposed; transpose=true, options...),
            () -> first(CSV.Chunks(input; options...)),
        )
        for read in readers
            empty!(CONVERSION_TASKS)
            CONVERSION_PEAK[] = 0
            file = read()
            @test all(column -> all(x -> String(x) == "text", column), Tables.Columns(file))
            @test CONVERSION_ACTIVE[] == 0
            @test 1 <= CONVERSION_PEAK[] <= budget
            @test length(CONVERSION_TASKS) <= budget
            budget == 1 && @test CONVERSION_TASKS == Set([current_task()])
        end
    end
end
