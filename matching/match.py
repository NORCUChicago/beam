'''
Core match module

This module loads preprocessed data, calculates similarities, accept matches,
and save all pairs evaluated and their match results for postprocessing.

Config.json includes all parameters of the input data and the match,
including the following:

- Match type (1:1, 1:M, M:M, dedup)
- Input file parameters
- Output directory
- Blocking strategies
- Similarity scores for partial matches

Usage:

From top-level repository, run
    python matching/match.py

'''

import json
import time
import gc
import os
import re
import datetime
import multiprocessing
import pandas as pd
from functools import partial

from record_linkage_shared import block_functions
from record_linkage_shared import match_functions

CONFIG_FILE = 'config.json' # Filepath to generated match configuration

if __name__ == "__main__":
    with open(CONFIG_FILE, "r") as f:
        config = json.loads(f.read())

    matchtype = config['matchtype']

    ### Load preprocessed data and match parameters ###  -----------------------
    print(f'Start time: {datetime.datetime.now()}')
    last_time = time.time()

    # Identify input data
    name_a = config['data_param']['df_a']['name']
    if matchtype == 'dedup':
        name_b = 'dedup'
        print(f'Loading preprocessed data for deduplication: {name_a}...')
    else:
        name_b = config['data_param']['df_b']['name']
        print(f'Loading preprocessed data... \n(A) {name_a}\n(B) {name_b}')

    # Load data (tablename_a referring to name of table in database)
    df_a, tablename_a = match_functions.load_data('df_a', config)
    vars_a = config['data_param']['df_a']['vars']

    if matchtype == 'dedup':
        df_b = None
        tablename_b = tablename_a
        vars_b = vars_a
    else:
        df_b, tablename_b = match_functions.load_data('df_b', config)
        vars_b = config['data_param']['df_b']['vars']

    print('Check column data types')
    print(f'{name_a}:\n', df_a.dtypes)
    if matchtype != 'dedup':
        print(f'{name_b}:\n', df_b.dtypes)

    match_functions.print_runtime(last_time)

    ### Initialize variables and database connection ###  ----------------------
    # Dataframe to store index pairs' similarity scores
    df_sim = pd.DataFrame(columns=['indv_id_a', 'indv_id_b', 'idx_a', 'idx_b'])

    # Connection to database
    db_info = config["database_information"]
    if db_info:
        conn, schema, _ = match_functions.connect_to_db(db_info)
        table_a = f'{schema}.{tablename_a}'
        table_b = f'{schema}.{tablename_b}'
    else:
        conn = None
        schema = None
        table_a = tablename_a
        table_b = tablename_b

    # String representing part of the SQL join conditions used for blocking.
    # This condition is updated dynamically each pass to prevent subsequent
    # passes from blocking on repeated pairs from previous passes.
    if conn:
        past_join_cond_str = ''
    else:
        past_join_cond_str = [[],[]]

    # Dataframe containing counts of matched pairs in each pass by strictness level
    counts = pd.DataFrame(columns=["pass_name", "strictness", "match"])

    # Pre-match: find pairs sharing ground truth IDs ### -----------------------
    last_time = time.time()
    if config["ground_truth_ids"]:
        # Cursor used to create, read and drop ground truth ids candidates tables
        gid_cursor = conn.cursor()
        gid_cursor.execute(f'SET ROLE {schema}admin;')
        past_join_cond_str = block_functions.run_ground_truth_ids_passes(
                                                            config["ground_truth_ids"],
                                                            vars_a, vars_b, schema,
                                                            name_a, name_b,
                                                            past_join_cond_str,
                                                            gid_cursor,
                                                            table_a, table_b)
        conn.commit()
        for g_id in config["ground_truth_ids"]:
            # Read in candidate table for each ground truth ID to save pairs
            matches = match_functions.read_in_pairs_sharing_gid(
                name_a=name_a,
                name_b=name_b,
                gid=g_id,
                cursor=gid_cursor,
                schema=schema)
            conn.commit()
            matches['pass_name'] = f'dup_{g_id}'
            # Assign ground truth ids weight to be greater than all other passes
            matches["weight"] = 10 ** (len(config["blocks_by_pass"]) + 1)
            df_sim = pd.concat([df_sim, matches], axis=0)

        # Calculate counts of each ground truth ID matches by strictness level
        counts = match_functions.calculate_pass_match_counts(df_sim, counts)

        # Store results to temporary file and create new empty dataframe
        df_sim.to_csv("temp_match_gid.csv", index=False)
        df_sim = pd.DataFrame(columns=['indv_id_a', 
                                       'indv_id_b', 
                                       'idx_a', 
                                       'idx_b'])
        match_functions.print_runtime(last_time)

    # Start Matching ### -------------------------------------------------------
    # Compare similarities and accept pairs as matches for each pass
    print("Starting matching: ", datetime.datetime.now())
    blank_df = pd.DataFrame(columns=['idx_a', 'idx_b'])
        # create dictionary of (comparer name: comparer object) for calculating
        # similarities based on comparers listed in config
    comps = match_functions.prepare_comparers(vars_a=vars_a, 
                                            vars_b=vars_b, 
                                            config=config)

        # set up process pool and DB connection
    chunk_sizes = config['parallelization_metrics']['chunk_sizes']
    num_processes = config['parallelization_metrics']['num_processes']
    process_pool = multiprocessing.Pool(processes=num_processes)
    run_match = partial(
            match_functions.run_match_for_candidate_set,
            df_a=df_a,
            df_b=df_b,
            df_sim=blank_df,
            std_varnames=list(vars_a),
            comps=comps,
            config=config
            )
    if conn:
        cursor = conn.cursor()
        cursor.execute(f'SET ROLE {schema}admin;')
    else:
        cursor = None

    output_vars = ["indv_id_a", "indv_id_b", "idx_a", "idx_b",
                    "pass_name", "match_strict", "match_moderate",
                    "match_relaxed", "match_review", "weight"]
    for comp_vars in config["comp_names_by_pass"]:
        for var in config["comp_names_by_pass"][comp_vars]:
            if var not in output_vars:
                output_vars.append(var)

    # Run match in parallel
    all_cands = []
    passes =  sorted(config['blocks_by_pass'])
    last_time = time.time()
    i = 0
    for pass_name in passes:
        passnum = int(re.sub(r'\D+', '', pass_name))
        # Complete blocking for pass
        past_join_cond_str  = block_functions.run_blocking_pass(
                                                    config["blocks_by_pass"],
                                                    pass_name,
                                                    vars_a, vars_b,
                                                    name_a, name_b,
                                                    past_join_cond_str,
                                                    table_a, table_b,
                                                    df_a, df_b,
                                                    schema, cursor)
        cand_table = f"candidates_{name_a}_{name_b}_p{pass_name}"
        print(f'Looking up {cand_table}...')
        if cursor:
            # Check if this blocking pass is skipped
            cursor.execute(
                f'''SELECT EXISTS (SELECT * FROM information_schema.tables
                WHERE table_schema = '{schema}' AND table_name = '{cand_table}');''')
            if not cursor.fetchone()[0]: # Table not found
                print(f'No candidate table for pass {pass_name}, skipping')
                continue
            conn.commit()
            # Read in match pairs and complete similarity checks
            with conn.cursor(f"passes_cursor_{pass_name}") as cur:
                cmd = f"SELECT indv_id_a, indv_id_b, idx_a, idx_b FROM {schema}.{cand_table}"
                cur.execute(cmd)
                # Read in block by chunks, running similiarity check once threshold hit
                while chunk := cur.fetchmany(size=chunk_sizes[str(passnum)]):
                    if not chunk:
                        break
                    candidates = pd.DataFrame(chunk,
                                            columns=['indv_id_a', 'indv_id_b',
                                                    'idx_a', 'idx_b'])
                    all_cands.append([candidates, pass_name])
                    if len(all_cands) == num_processes * 2:
                        dfs = process_pool.starmap(run_match, tuple(all_cands))
                        dfs = pd.concat(dfs, ignore_index=True)
                        counts = match_functions.calculate_pass_match_counts(dfs, 
                                                                             counts)
                        match_functions.calculate_weights(dfs, len(passes))
                        # Save out sorted dataframe to temp file
                        dfs = dfs.sort_values("weight", ascending=False)
                        dfs.reindex(columns=output_vars).to_csv(f"temp_match_{i}.csv",
                                                                index=False)
                        all_cands = []
                        i += 1
                        gc.collect()
            # Drop candidates table for current pass once done using
            cmd = f'''DROP TABLE IF EXISTS {schema}.{cand_table}'''
            cursor.execute(cmd)
            conn.commit()
        else:
            blockfile = f'{cand_table}.csv'
            dtypes = {"idx_a": int, 
                      "idx_b": int, 
                      "indv_id_a":str, 
                      "indv_id_b": str}
            for chunk in pd.read_csv(blockfile, 
                                     chunksize=chunk_sizes[str(passnum)], 
                                     dtype=dtypes):
                if not chunk.empty:
                    all_cands.append([chunk, pass_name])
                if len(all_cands) == num_processes * 2:
                    dfs = process_pool.starmap(run_match, tuple(all_cands))
                    dfs = pd.concat(dfs, ignore_index=True)
                    counts = match_functions.calculate_pass_match_counts(dfs, 
                                                                         counts)
                    match_functions.calculate_weights(dfs, len(passes))
                    # Save out sorted dataframe to temp file
                    dfs = dfs.sort_values("weight", ascending=False)
                    dfs.reindex(columns=output_vars).to_csv(f"temp_match_{i}.csv",
                                                            index=False)
                    all_cands = []
                    i += 1
                    gc.collect()
           #  os.remove(blockfile)
    # Compute similarity check for any remaining candidates not yet processed
    if all_cands:
        dfs = process_pool.starmap(run_match, tuple(all_cands))
        dfs = pd.concat(dfs, ignore_index=True)
        counts = match_functions.calculate_pass_match_counts(dfs, counts)
        match_functions.calculate_weights(dfs, len(passes))
        dfs = dfs.sort_values("weight", ascending=False)
        dfs.reindex(columns=output_vars).to_csv(f"temp_match_{i}.csv", 
                                                index=False)
        all_cands = []
        gc.collect()
    if conn:
        conn.close()
    match_functions.print_runtime(last_time)

    print("All passes completed.")
    last_time = time.time()
    match_functions.print_runtime(last_time)
    process_pool.close()
    gc.collect()
    counts = counts.reset_index(drop=True)

    print("Ending matching: ", datetime.datetime.now())
    print("All similarity calculation complete.")

    ### Print match counts ### -------------------------------------------------
    gid_passes = ['dup_' + gid for gid in config['ground_truth_ids']]
    all_passes = gid_passes + passes

    # Per pass
    for p in all_passes:
        match_functions.print_match_count(counts, pass_name=p)
    # Entire match
    match_functions.print_match_count(counts)

    ### Save output ###  -------------------------------------------------------
    print('Saving output...')
    match_functions.save_output(name_a, name_b, config)

    print(f'End time: {datetime.datetime.now()}')
