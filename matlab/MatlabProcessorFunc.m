function results = MatlabProcessorFunc(funcNames, paramSets)
    n = length(funcNames);

    maxWorkers = max(1, feature('numcores') - 1);
    results = cell(n, 1);

    p = gcp('nocreate');
    % TODO: no parpool recreation if maxWorkers changes
    if isempty(p)
        p = parpool('Threads', maxWorkers);
    end

    % TODO: parfeval may perform better than parfor
    % when the number of tasks is greater than or equal to the number of workers
    parfor (ii = 1:n, parforOptions(p))
        try
            fhandle = str2func(funcNames{ii});
            results{ii} = fhandle(paramSets{ii}{:});
        catch e
            results{ii} = getReport(e);
        end
    end
end
