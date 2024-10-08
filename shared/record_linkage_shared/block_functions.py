'''
This script contains functions to create the blocks for
the matching.
'''
import json
import time
import psycopg2
import recordlinkage
import pandas as pd
import numpy as np

from recordlinkage.index import Block
from recordlinkage.base import BaseIndexAlgorithm
from recordlinkage.utils import listify

start = time.time()

CONFIG_FILE = "config.json"


def get_pass_join_cond(passblocks_a, passblocks_b):
    # create the "ON" part of a join command
    join_cond = []
    for i in range(len(passblocks_a)):
        cond = f'''
            (a.{passblocks_a[i]} = b.{passblocks_b[i]} \
            AND a.{passblocks_a[i]} != ''
            AND a.{passblocks_a[i]} IS NOT NULL)
            '''
        join_cond.append(cond)
    join_cond_str = ' AND '.join(join_cond)

    return join_cond_str


def exclude_past_join_cond(join_cond_str, past_join_cond_str):
    '''exclude pairs that are captured by past blocking strategies
    from the new join'''
    if past_join_cond_str:
        join_cond_fullstr = join_cond_str + f' AND NOT ({past_join_cond_str})'
    else:
        join_cond_fullstr = join_cond_str

    return join_cond_fullstr


def update_past_join_cond(join_cond_str, past_join_cond_str):
    '''update past_join_cond_str to include the current pass' join conditions
    '''
    if past_join_cond_str:
        past_join_cond_str = past_join_cond_str + f' OR ({join_cond_str})'
    else:
        past_join_cond_str = f'({join_cond_str})'

    return past_join_cond_str


def execute_blocking_join(table_a, table_b, candidates_table, indv_id_a,
                          indv_id_b, join_cond_fullstr, cursor):
    '''
    Drop candidates table for pass, if exists, and execute command
    to create the candidates table using the join condition.
    '''
    cmd = '''DROP TABLE IF EXISTS {}'''.format(candidates_table)
    cursor.execute(cmd)

    cmd = '''
    CREATE TABLE IF NOT EXISTS {candidates_table} AS
    SELECT
        a.{indv_id_a} as indv_id_a,
        b.{indv_id_b} as indv_id_b,
        a.idx::integer AS idx_a,
        b.idx::integer AS idx_b
    FROM (SELECT * FROM {table_a} ORDER BY idx::float) a
    INNER JOIN (SELECT * FROM {table_b} ORDER BY idx::float) b
    ON {join_cond_fullstr}
    ;'''.format(
        candidates_table=candidates_table,
        indv_id_a=indv_id_a,
        indv_id_b=indv_id_b,
        table_a=table_a,
        table_b=table_b,
        join_cond_fullstr=join_cond_fullstr
        )
    cursor.execute(cmd)


def find_pass_candidates(passblocks_a, passblocks_b, indv_id_a, indv_id_b,
                         past_join_cond_str, table_a, table_b,
                         candidates_table, dedup, cursor):
    '''defines join conditions, creates a candidate table with rows
    (idx_a, idx_b) in Postgres, and updates the string that stores
    past join conditions, which are excluded in the next blocking pass
    '''

    join_cond_str = get_pass_join_cond(passblocks_a, passblocks_b)
    join_cond_fullstr = exclude_past_join_cond(join_cond_str,
                                               past_join_cond_str)
    if dedup:
        # exclude joining to self
        join_cond_fullstr += 'AND a.idx < b.idx'
        join_cond_fullstr += f' AND a.{indv_id_a} != b.{indv_id_b}'
    execute_blocking_join(table_a, table_b, candidates_table,
                          indv_id_a, indv_id_b, join_cond_fullstr, cursor)

    # update past join conditions to exclude in the next pass
    past_join_cond_str = update_past_join_cond(join_cond_str,
                                               past_join_cond_str)

    return past_join_cond_str


def run_ground_truth_ids_passes(ground_truth_ids, vars_a, vars_b, schema,
                              name_a, name_b, past_join_cond_str,
                              cursor, table_a, table_b):
    '''
    Blocks on each ground truth ID provided and stores all the matching
    pairs in a candidate table.

    Inputs:
        ground_truth_ids (list)
        vars_a (dict): source variable names for df a
        vars_b (dict): source variable names for df b
        schema (str): name of schema where candidates tables are stored
        name_a (str): name of df a (for storing/print purposes)
        name_b (str): name of df b (for storing/print purposes)
        past_join_cond_str (str): string of past join combinations to avoid
            matching pairs repeatedly
        cursor (Cursor object): cursor connected to database
        table_a (str): database table name of df_a
        table_b (str): database table name of db_b

    Returns (str) updated past_join_cond_str
    '''
    print('Finding pairs sharing ground truth IDs...')
    tot_gid_cnt = len(ground_truth_ids)

    indv_id_a = vars_a['indv_id']
    indv_id_b = vars_b['indv_id']

    if name_b == "dedup":
        match_name = f"{name_a}_dedup"
        table_b = table_a
        dedup = True
    else:
        match_name = f"{name_a}_{name_b}"
        dedup = False

    for i in range(tot_gid_cnt):
        gid = ground_truth_ids[i]
        print(f'- {gid}')
        gid_start = time.time()

        # get actual ground truth ID variable names for each dataset
        gid_a = vars_a[gid]
        gid_b = vars_b[gid]

        candidates_table = f'{schema}.candidates_{match_name}_matching_{gid}'

        past_join_cond_str = find_pass_candidates([gid_a], [gid_b],
                                                  indv_id_a, indv_id_b,
                                                  past_join_cond_str,
                                                  table_a, table_b,
                                                  candidates_table, dedup,
                                                  cursor)

        gid_end = time.time()
        print('***Table: {}'.format(candidates_table))
        print('***Rows inserted: {:,}'.format(cursor.rowcount))
        print('***Time: ', gid_end - gid_start)
    return past_join_cond_str


class CustomIndex(BaseIndexAlgorithm):
    def __init__(self, left_on=None, right_on=None, left_seen=None, right_seen=None):
        super(CustomIndex, self).__init__()

        # variables to block on
        self.left_on = left_on
        self.right_on = right_on
        
        # variables that have already been blocked on
        self.left_seen = left_seen
        self.right_seen = right_seen

    def _get_left_and_right_on(self):
        if self.right_on is None:
            return (self.left_on, self.left_on)
        else:
            return (self.left_on, self.right_on)

    def _get_left_and_right_seen(self):
        if self.right_seen is None:
            return (self.left_seen, self.left_seen)
        else:
            return (self.left_seen, self.right_seen)
        
    def _link_index(self, df_a, df_b):
        left_on, right_on = self._get_left_and_right_on()
        left_seen, right_seen = self._get_left_and_right_seen()
        left_on = listify(left_on)
        right_on = listify(right_on)

        blocking_keys = ["blocking_key_%d" % i for i, v in enumerate(left_on)]
        left_keep = list(set(var for v in left_seen for var in v))
        right_keep = list(set(var for v in right_seen for var in v))
        
        # make a dataset for the data on the left
        data_left = pd.DataFrame(df_a[left_on + left_keep], copy=False)
        data_left.columns = blocking_keys + left_keep
        data_left["index_x"] = np.arange(len(df_a))
        data_left.dropna(axis=0, how="any", subset=blocking_keys, inplace=True)

        # make a dataset for the data on the right
        data_right = pd.DataFrame(df_b[right_on + right_keep], copy=False)
        data_right.columns = blocking_keys + right_keep
        data_right["index_y"] = np.arange(len(df_b))
        data_right.dropna(axis=0, how="any", subset=blocking_keys, inplace=True)

        # merge the dataframes
        pairs_df = data_left.merge(data_right, how="inner", on=blocking_keys)
        for i, prev_block_l in enumerate(left_seen):
            prev_block_r = right_seen[i]
            statement = True
            for j, left_var in enumerate(prev_block_l):
                right_var = prev_block_r[j]
                if left_var in data_right.columns or right_var in data_left.columns:
                    left_var = left_var + "_x"
                    right_var = right_var + "_y"
                statement = statement & (pairs_df[left_var] == pairs_df[right_var])
            pairs_df = pairs_df[~statement]
        pairs_df = pairs_df[blocking_keys + ["index_x", "index_y"]]

        return pd.MultiIndex(
            levels=[df_a.index.values, df_b.index.values],
            codes=[pairs_df["index_x"].values, pairs_df["index_y"].values],
            verify_integrity=False,
        )



def find_block(data_a, data_b, blocking_vars_a, blocking_vars_b, dedup,
               prev_vars_a, prev_vars_b, candidates_table):
    '''Calculate blocking pass.
    
    Calculate a blocking pass for the given blocking variables. All records
    with empty string values for any of the blocking variables will be 
    ignored.
    
    Args:
        df (DataFrame): Input dataset.
        blocking_vars ((str)): Tuple of column names to be blocked on.
    Returns:
        Multindex pairs
        
    '''
    # Create indexer.
    if dedup:
        indexer = CustomIndex(left_on=blocking_vars_a, left_seen=prev_vars_a)
    else:
        indexer = CustomIndex(left_on=blocking_vars_a, right_on=blocking_vars_b,
                      left_seen=prev_vars_a, right_seen=prev_vars_b)
    
    # Format query string. This query string removes rows from 
    # the input data that have empty strings for any of the
    # blocking variables.
    q_a = ' != "") and ('.join(blocking_vars_a)
    q_a = '(' + q_a + '!= "")'
    q_b = ' != "") and ('.join(blocking_vars_b)
    q_b = '(' + q_b + '!= "")'
    try:
        if dedup:
            pairs = indexer.index(
                data_a.query(
                    q_a
                )
                )
        else:
            pairs = indexer.index(
                data_a.query(
                    q_a
                ),
                data_b.query(
                    q_b
                )
            )
    except ZeroDivisionError:
        # assign empty object if no pairs meet blocking criteria
        pairs = pd.MultiIndex(levels=[[]], codes=[[]])
    
    df = make_df(pairs)
    rows = df.shape[0]
    df.to_csv(candidates_table, index=False)
    prev_vars_a.append(blocking_vars_a)
    if blocking_vars_b:
        prev_vars_b.append(blocking_vars_b)
    return (prev_vars_a, prev_vars_b), rows


def remove_previously_seen_pairs(new_pairs, new_name, old_pairs):
    '''Remove previously seen pairs from Multindex of new pairs.
    
    Args:
        new_pairs (multindex): New pairs.
        new_name (str): name of new Multindex of pairs.
        old_pairs ({str: multindex]}): Map of multindex name to list of pairs.
        
    Returns:
        Multiindex of pairs.
        
    '''
    
    for name, pair in old_pairs.items():
        if (name != new_name) & (len(pair) != 0):
            new_pairs = new_pairs.difference(pair)
        
    return new_pairs


def run_block(data_a, data_b, blocking_vars_a, blocking_vars_b, pair_name, 
              all_pairs, dedup, candidates_table):
    '''Run all steps in a blocking pass for the given blocking variables.
    
    Run all steps in a blocking pass for the given blocking variables 
    including removing previously seen pairs. Updates dictionary of all pairs
    (all_pairs.)
    
    Args:
        data (DataFrame): Input dataset.
        blocking_vars ((str)): Tuple of column names to be blocked on.
        pair_name (str): Name of new Multindex of pairs.
        all_pairs ({str: multindex]}): Map of multindex name to list of pairs.
    
    Returns:
        (Multindex of new pairs, [Multindex of pairs from previous blocks])
        
    '''
    
    all_pairs = find_block(data_a, data_b, blocking_vars_a, blocking_vars_b, dedup,
                       all_pairs[0], all_pairs[1], candidates_table)
    
    return all_pairs


def make_df(all_pairs):
    '''Transforms multindex pairs in preparation for saving to file.
    
    - Converts multindexes to dataframes.
    - Adds "blocking pass" column with block number.
    - Concatenates all dataframes into one data frame.
    - Gets rid of extra index columns.
    
    Args:
        all_pairs ({str: multindex]}): Map of multindex name to list of pairs.
        pass_name_to_number ({str: int}): Map of block name to number.
        
    Returns: 
        DataFrame with all pairs.
        
    '''
    
    # all_df = []
    # for name, pairs in all_pairs.items():
    #     if len(pairs) != 0:
    #         block = pairs.to_frame()
    #         block_n = pass_name_to_number[name]
    #         block['blocking_pass'] = block_n
    #         all_df.append(block)

    # out = pd.concat(all_df)
    out = all_pairs.to_frame().reset_index(drop=True) 
    if out.empty:
        return pd.DataFrame([], columns=["indv_id_a", "indv_id_b", "idx_a", "idx_b"])
    out["indv_id_a"], out["idx_a"] = zip(*out[0])
    out["indv_id_b"], out["idx_b"] = zip(*out[1])
    out = out[['indv_id_a', 'indv_id_b', 'idx_a', 'idx_b']]
    return out

def run_blocking_pass(blocks_by_pass, passnum, vars_a, vars_b,
                      name_a, name_b, past_join_cond_str,
                      table_a, table_b, df_a, df_b, schema=None, cursor=None):
    '''
    Blocks on pass provided and stores all the matching
    pairs in a candidate table.

    Inputs:
        blocks_by_pass (list): list of each pass, containing variables to block
            on for each pass
        passnum (int): The current pass
        vars_a (dict): source variable names for df a
        vars_b (dict): source variable names for df b
        schema (str): name of schema where candidates tables are stored
        name_a (str): name of df a
        name_b (str): name of df b
        past_join_cond_str (str): string of past join combinations to avoid
            matching pairs repeatedly
        cursor (Cursor object): cursor connected to database
        table_a (str): database table name of df_a
        table_b (str): database table name of db_b

    Returns (str) updated past_join_cond_str
    '''
    blocking_vars = blocks_by_pass[passnum]

    indv_id_a = vars_a['indv_id']
    indv_id_b = vars_b['indv_id']

    if not cursor:
        indexer = recordlinkage.Index()
        df_a.set_index([indv_id_a, "idx"], inplace=True)
        if name_b != "dedup":
            df_b.set_index([indv_id_b, "idx"], inplace=True)

        
    if name_b == "dedup":
        match_name = f"{name_a}_dedup"
        table_b = table_a
        dedup = True
    else:
        match_name = f"{name_a}_{name_b}"
        dedup = False

    if blocking_vars:
        print(f"Pass {passnum} - Blocking on: {', '.join(blocking_vars)}")
        pass_start = time.time()

        # Flag if this pass is blocked on variables inverted
        # (e.g. xf/xl, xl/xf)
        inverted_blocks = '_inv' in str(blocking_vars)
        if inverted_blocks:
            blocking_vars = [s.replace('_inv', '') for s in blocking_vars]

        # get actual blocking variable names for each dataset
        passblocks_a = [vars_a[v] for v in blocking_vars if v in vars_a]
        passblocks_b = [vars_b[v] for v in blocking_vars if v in vars_b]

        if (len(passblocks_a) != len(blocking_vars) or
            len(passblocks_b) != len(blocking_vars)):
            missing_var = {v for v in blocking_vars if v not in vars_a or v not in vars_b}
            print(f"\tPass {passnum} is being skipped since {missing_var} is not included.")
            return past_join_cond_str
        if inverted_blocks:
            passblocks_b.reverse()
        if cursor:
            candidates_table = f'{schema}.candidates_{match_name}_p{passnum}'
            past_join_cond_str = find_pass_candidates(passblocks_a, passblocks_b,
                                                    indv_id_a, indv_id_b,
                                                    past_join_cond_str,
                                                    table_a, table_b,
                                                    candidates_table, dedup,
                                                    cursor)
        else:
            candidates_table = f'candidates_{match_name}_p{passnum}.csv'
            past_join_cond_str, rows = run_block(df_a, df_b, passblocks_a, passblocks_b, 
                               passnum, past_join_cond_str, dedup, candidates_table)
            df_a.reset_index(inplace=True)
            if name_b != "dedup":
                df_b.reset_index(inplace=True)
        pass_end = time.time()
        if cursor:
            rows = cursor.rowcount
        print('***Table: {}'.format(candidates_table))
        print('***Rows inserted: {:,}'.format(rows))
        print('***Time: ', pass_end - pass_start)
        return past_join_cond_str

    else:
        print(f"Pass {passnum} - Skipped according to config_match")
        return ''
