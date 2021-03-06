from glob import glob
#import hddm mv commented
import itertools
#from joblib import Parallel, delayed mv commented
#import kabuki mv commented
from math import ceil
import multiprocessing
import numpy
import os
import pandas
import pickle

def not_regex(txt):
    return '^((?!%s).)*$' % txt

def unique(lst):
    return list(numpy.unique(lst))


def EZ_diffusion(df, condition = None):
    assert 'correct' in df.columns, 'Could not calculate EZ DDM'
    df = df.copy()
    # convert reaction time to seconds to match with HDDM
    df['rt'] = df['rt']/1000
    # ensure there are no missed responses or extremely short responses (to fit with EZ)
    df = df.query('rt > .05')
    # convert any perfect accuracies to .95

    EZ_dvs = {}
    # calculate EZ params for each condition
    if condition:
        conditions = df[condition].unique()
        conditions = conditions[~pandas.isnull(conditions)]
        for c in conditions:
            subset = df[df[condition] == c]
            pc = subset['correct'].mean()
            # edge case correction using the fourth suggestion from
            # Stanislaw, H., & Todorov, N. (1999). Calculation of signal detection theory measures.
            if pc == 1:
                pc = 1-(.5/len(subset))
            vrt = numpy.var(subset.query('correct == True')['rt'])
            mrt = numpy.mean(subset.query('correct == True')['rt'])
            try:
                drift, thresh, non_dec = hddm.utils.EZ(pc, vrt, mrt)
                EZ_dvs['EZ_drift_' + c] = {'value': drift, 'valence': 'Pos'}
                EZ_dvs['EZ_thresh_' + c] = {'value': thresh, 'valence': 'Pos'}
                EZ_dvs['EZ_non_decision_' + c] = {'value': non_dec, 'valence': 'Neg'}
            except ValueError:
                continue
    else:
        # calculate EZ params
        try:
            pc = df['correct'].mean()
            # edge case correct
            if pc == 1:
                pc = 1-(1.0/(2*len(df)))
            vrt = numpy.var(df.query('correct == True')['rt'])
            mrt = numpy.mean(df.query('correct == True')['rt'])
            drift, thresh, non_dec = hddm.utils.EZ(pc, vrt, mrt)
            EZ_dvs['EZ_drift'] = {'value': drift, 'valence': 'Pos'}
            EZ_dvs['EZ_thresh'] = {'value': thresh, 'valence': 'Pos'}
            EZ_dvs['EZ_non_decision'] = {'value': non_dec, 'valence': 'Neg'}
        except ValueError:
            return {}
    return EZ_dvs

def parallel_sample(db_name, hddm_fun, hddm_args, samples, burn, thin):
        """ feed function into parallel to parallelize HDDM fits """
        # find a good starting point which helps with the convergence.
        print('Running %s' % db_name)
        m = hddm_fun(**hddm_args)
        m.find_starting_values()
        m.sample(samples, burn=burn, thin=thin, dbname=db_name, db='pickle')
        return m

def load_concat_models(models):
    """Concatenate traces of multiple identical models into a new
    model containing all traces of the individual models.

    """
    # copy first model
    target_model = models[0]
    target_stochs = target_model.get_stochastics()
    # append traces
    for i, model in enumerate(models[1:]):
        stochs = model.get_stochastics()
        for node, target_node in zip(stochs.node, target_stochs.node):
            assert node.__name__ == target_node.__name__, "Node names do not match. You have to pass identical models."
            target_node.trace._trace[0] = numpy.concatenate([target_node.trace[:], node.trace[:]])
    return target_model

def load_model(empty_model, dbfile):
    loadfile = sorted(glob(dbfile))
    if len(loadfile) > 1:
        models = []
        for l in loadfile:
            m = hddm.load(empty_model)
            m.load_db(l, db='pickle')
            models.append(m)
        m = load_concat_models(models)
        return m, models
    else:
        m = hddm.load(empty_model)
        m.load_db(loadfile[0], db='pickle')
        return m

def fit_HDDM(df,
             response_col = 'correct',
             categorical_dict = {},
             parametric_dict = {},
             formulas = None,
             outfile = None,
             samples=95000,
             burn=15000,
             thin=1,
             parallel=False,
             num_cores=None):
    """ wrapper to run hddm analysis

    Args:
        df: dataframe to perform hddm analyses on
        respones_col: the columnof correct/incorrect values
        formulas_cols: (optional) single dictionary, orlist of dictionaries,
            whose key is a hddm param
            The  values of each dictare column names to be used in a regression model
            If none are passed, no regression will be performed. For instance,
            if categorical_dict = [{'v': ['condition1']}] then a regression will be
            run of the form: "v ~ C(condition1, Sum)"
        formulas: (optional) if given overrides automatic formulas
        outfile: if given, models will be saved to this location
        db_loc: optional. If running locally, this option is unnecessary. If
            running in a container, however, the database location for each model
            needs to be changed to the local save location of the models. Thus
            enter the directory where you are locally saving.
        samples: number of samples to run HDDM
        burn: burn in time for HDDM
        thin: thin parameter passed to HDDM
        parallel: whether to run HDDM in parallel. If run in parallel, the final
            model will still have at least the original final number of samples:
                (samples-burn)/thin
        num_cores: the number of cores to use for parallelization. If not set will
            use all cores
    """
    variable_conversion = {'a': ('thresh', 'Pos'), 'v': ('drift', 'Pos'), 't': ('non_decision', 'NA')}
    db = None
    extra_cols = []
    categorical_cols = []
    parametric_cols = []
    # set up data
    data = (df.loc[:,'rt']/1000).astype(float).to_frame()
    data.insert(0, 'response', df[response_col].astype(float))
    for var in ['a','t','v','z']:
        parametric_cols += parametric_dict.get(var, [])
        categorical_cols += categorical_dict.get(var, [])
    categorical_cols = unique(categorical_cols)
    parametric_cols = unique(parametric_cols)
    extra_cols = parametric_cols + categorical_cols
    extra_cols = unique(extra_cols)
    for col in extra_cols:
        data.insert(0, col, df[col])
    # state cols dropped when using deviance coding
    dropped_vals = [sorted(data[col].unique())[-1] for col in categorical_cols]
    # add subject ids
    data.insert(0,'subj_idx', df['worker_id'])
    # remove missed responses and extremely short response
    data = data.query('rt > .05')
    subj_ids = data.subj_idx.unique()
    ids = {subj_ids[i]:int(i) for i in range(len(subj_ids))}
    data.replace(subj_ids, [ids[i] for i in subj_ids],inplace = True)
    if outfile:
        db = outfile + '_traces.db'
    # run if estimating variables for the whole task
    if len(extra_cols) == 0:
        if parallel:
            hddm_fun = hddm.HDDM
            hddm_args = {'data': data}
        m = hddm.HDDM(data)
    else:
        # if no explicit formulas have been set, create them
        if formulas is None:
            formulas = []
            # iterate through formula cols
            for ddm_var in ['a','t','v','z']:
                formula = ''
                cat_cols = categorical_dict.get(ddm_var, [])
                if len(cat_cols) > 0:
                    regressor = 'C(' + ', Sum)+C('.join(cat_cols) + ', Sum)'
                    formula = '%s ~ %s' % (ddm_var, regressor)
                par_cols = parametric_dict.get(ddm_var, [])
                if len(par_cols) > 0:
                    regressor = ' + '.join(par_cols)
                    if formula == '':
                        formula = '%s ~ %s' % (ddm_var, regressor)
                    else:
                        formula += ' + ' + regressor
                if formula != '':
                    formulas.append(formula)


        if parallel == True:
            hddm_fun = hddm.models.HDDMRegressor
            hddm_args = {'data': data,
                         'models': formulas,
                         'group_only_regressors': False}

        m = hddm.models.HDDMRegressor(data, formulas,
                                      group_only_regressors=False)

    if outfile:
        empty_path = outfile + '_empty.model'
        m.save(empty_path)
    # run model
    if parallel==True:
        if num_cores is None:
            num_cores = multiprocessing.cpu_count()
        assert outfile is not None, "Outfile must be specified to parallelize"
        # create folder for parallel traces
        parallel_dir = outfile + '_parallel_output'
        num_parallel_dirs = len(glob(parallel_dir+'*'))
        if num_parallel_dirs > 0:
            parallel_dir += '_%s' % str(num_parallel_dirs+1)
        os.makedirs(parallel_dir, exist_ok=True)
        # set db names
        dbs = [db[:-3]+'%s.db' % i for i in range(1,num_cores+1)]
        dbs = [os.path.join(parallel_dir, os.path.basename(i)) for i in dbs]
        # set sample number for parallel run
        parallel_samples = ceil((samples-burn)/num_cores)+burn
        print('Parallelizing using %s cores. %s samples each' % (str(num_cores), str(parallel_samples)))
        # run models
        results = Parallel(n_jobs=num_cores)(delayed(parallel_sample)(i, hddm_fun, hddm_args, parallel_samples, burn, thin) for i in dbs)
        print('Separate Models Run, Concatenating...')
        m = kabuki.utils.concat_models(results)
        print('Finished Concatenating')
    else:
        # find a good starting point which helps with the convergence.
        m.find_starting_values()
        m.sample(samples, burn=burn, thin=thin, dbname=db, db='pickle')
    if outfile:
        try:
            if parallel==True:
                for i, sub_m in enumerate(results):
                    base = os.path.basename(outfile) + '_%s.model' % str(i+1)
                    save_loc = os.path.join(parallel_dir, base)
                    pickle.dump(sub_m, open(save_loc, 'wb'))
            else:
                pickle.dump(m, open(outfile + '.model', 'wb'))
        except Exception:
            print('Saving model failed')

    # get average ddm params
    # regex match to find the correct rows
    dvs = {}
    for var in ['a','v','t']:
        match = '^'+var+'(_subj|_Intercept_subj)'
        dvs[var] = m.nodes_db.filter(regex=match, axis=0)['mean']

    # output of regression (no interactions)
    condition_dvs = {}
    for ddm_var in ['a','v','t']:
        var_dvs = {}
        # add categorical ddm vars
        for col, dropped in zip(categorical_cols, dropped_vals):
            col_dvs = {}
            included_vals = [i for i in data[col].unique() if i != dropped]
            for val in included_vals:
                # regex match to find correct rows
                match='^'+ddm_var+'.*S.'+str(val)+']_subj'
                # get the individual diff values and convert to list
                ddm_vals = m.nodes_db.filter(regex=match, axis=0).filter(regex=not_regex(':'), axis=0)['mean'].tolist()
                if len(ddm_vals) > 0:
                    col_dvs[val] = ddm_vals
            if len(col_dvs.keys()) > 0:
                # construct dropped dvs
                dropped_dvs = []
                for vs in zip(*col_dvs.values()):
                    dropped_dvs.append(-1*sum(vs))
                col_dvs[dropped] = dropped_dvs
            var_dvs.update(col_dvs)
        # add parametric ddm vars
        for col in parametric_cols:
            col_dvs = {}
            # regex match to find correct rows
            match='^'+ddm_var+'_'+col+'_subj'
            # get the individual diff values and convert to list
            ddm_vals = m.nodes_db.filter(regex=match, axis=0).filter(regex=not_regex(':'), axis=0)['mean'].tolist()
            if len(ddm_vals) > 0:
                col_dvs[col] = ddm_vals
            var_dvs.update(col_dvs)
        if len(var_dvs)>0:
            condition_dvs[ddm_var] = var_dvs

    # interaction
    interaction_dvs = {}
    all_levels = []
    for col in extra_cols:
        all_levels += list(data.loc[:,col].unique())
    for ddm_var in ['a','v','t']:
        var_dvs = {}
        for x, y in itertools.permutations(all_levels,2):
            # regex match to find correct rows
            match='^'+ddm_var+'.*'+str(x)+'].*:.*'+str(y)+']_subj'
            # get the individual diff values and convert to list
            ddm_vals = m.nodes_db.filter(regex=match, axis=0)['mean'].tolist()
            if len(ddm_vals) > 0:
                var_dvs['%s:%s' % (str(x), str(y))] = ddm_vals
        if len(var_dvs) > 0:
            interaction_dvs[ddm_var] = var_dvs

    group_dvs = {}
    # create output ddm dict
    for i,subj in enumerate(subj_ids):
        group_dvs[subj] = {}
        hddm_vals = {}
        for var in ['a','v','t']:
            var_name, var_valence = variable_conversion[var]
            if var in list(dvs.keys()):
                hddm_vals.update({'hddm_' + var_name: {'value': dvs[var][i], 'valence': var_valence}})
            if var in condition_dvs.keys():
                for k,v in condition_dvs[var].items():
                    tmp = {'value': v[i], 'valence': var_valence}
                    hddm_vals.update({'hddm_'+var_name+'_'+str(k): tmp})
            if var in interaction_dvs.keys():
                for k,v in interaction_dvs[var].items():
                    tmp = {'value': v[i], 'valence': var_valence}
                    hddm_vals.update({'hddm_'+var_name+'_'+k: tmp})
        group_dvs[subj].update(hddm_vals)

    return group_dvs

def ANT_HDDM(df,  **kwargs):
    group_dvs = fit_HDDM(df,
                         categorical_dict = {'v': ['flanker_type', 'cue']},
                         **kwargs)
    return group_dvs

def directed_HDDM(df,  **kwargs):
    n_responded_conds = df.query('rt>.05').groupby('worker_id').probe_type.unique().apply(len)
    complete_subjs = list(n_responded_conds.index[n_responded_conds==3])
    missing_subjs = set(n_responded_conds.index)-set(complete_subjs)
    if len(missing_subjs) > 0:
        print('Subjects without full design matrix: %s' % missing_subjs)
    df = df.query('worker_id in %s' % complete_subjs)
    group_dvs = fit_HDDM(df.query('trial_id == "probe"'),
                          categorical_dict = {'v': ['probe_type']},
                          **kwargs)
    return group_dvs

def DPX_HDDM(df,  **kwargs):
    n_responded_conds = df.query('rt>0').groupby('worker_id').condition.unique().apply(len)
    complete_subjs = list(n_responded_conds.index[n_responded_conds==4])
    missing_subjs = set(n_responded_conds.index)-set(complete_subjs)
    if len(missing_subjs) > 0:
        print('Subjects without full design matrix: %s' % missing_subjs)
    df = df.query('worker_id in %s' % complete_subjs)
    group_dvs = fit_HDDM(df,
                          categorical_dict = {'v': ['condition']},
                          **kwargs)
    return group_dvs

def motor_SS_HDDM(df, mode='proactive', **kwargs):
    df = df.copy()
    critical_key = (df.correct_response == df.stop_response).map({True: 'critical', False: 'non-critical'})
    df.insert(0, 'critical_key', critical_key)
    if mode == 'proactive':
        # proactive control
        df = df.query('SS_trial_type == "go" and \
                     exp_stage not in ["practice","NoSS_practice"]')
        group_dvs = fit_HDDM(df,
                             categorical_dict = {'v': ['critical_key']},
                             **kwargs)
    elif mode == 'reactive':
        # reactive control
        df = df.query('condition != "stop" and critical_key == "non-critical" and \
                        exp_stage not in ["practice","NoSS_practice"]')
        group_dvs = fit_HDDM(df,
                             categorical_dict = {'v': ['condition']},
                             **kwargs)
    elif mode == 'both':
        pdf = df.query('SS_trial_type == "go" and \
                 exp_stage not in ["practice","NoSS_practice"]')
        pgroup_dvs = fit_HDDM(pdf,
                         categorical_dict = {'v': ['critical_key']},
                         **kwargs)
        # reactive control
        rdf = df.query('condition != "stop" and critical_key == "non-critical" and \
                        exp_stage not in ["practice","NoSS_practice"]')
        rgroup_dvs = fit_HDDM(rdf,
                             categorical_dict = {'v': ['condition']},
                             **kwargs)
        # this ends up using the pgroup_dvs for the base threshold, drift and non-decision time
        group_dvs = rgroup_dvs
        for key, value in group_dvs.items():
            value.update(pgroup_dvs[key])

    return group_dvs


def recent_HDDM(df,  **kwargs):
    n_responded_conds = df.query('rt>.05').groupby('worker_id').probeType.unique().apply(len)
    complete_subjs = list(n_responded_conds.index[n_responded_conds==4])
    missing_subjs = set(n_responded_conds.index)-set(complete_subjs)
    if len(missing_subjs) > 0:
        print('Subjects without full design matrix: %s' % missing_subjs)
    df = df.query('worker_id in %s' % complete_subjs)
    group_dvs = fit_HDDM(df,
                          categorical_dict = {'v': ['probeType']},
                          **kwargs)
    return group_dvs

def shape_matching_HDDM(df, **kwargs):
    # restrict to the conditions of interest
    df = df.query('condition in %s' % ['SDD', 'SNN'])
    n_responded_conds = df.query('rt>.05').groupby('worker_id').condition.unique().apply(len)
    complete_subjs = list(n_responded_conds.index[n_responded_conds==2])
    missing_subjs = set(n_responded_conds.index)-set(complete_subjs)
    if len(missing_subjs) > 0:
        print('Subjects without full design matrix: %s' % missing_subjs)
    df = df.query('worker_id in %s' % complete_subjs)
    group_dvs = fit_HDDM(df,
                          categorical_dict = {'v': ['condition']},
                          **kwargs)
    return group_dvs

def stim_SS_HDDM(df, **kwargs):
    df = df.query('condition != "stop" and \
                 exp_stage not in ["practice","NoSS_practice"]')
    group_dvs = fit_HDDM(df,
                         categorical_dict = {'v': ['condition']},
                         **kwargs)
    return group_dvs

def SS_HDDM(df, **kwargs):
    df = df.query('SS_trial_type == "go" \
                 and exp_stage not in ["practice","NoSS_practice"]')
    if 'condition' in df.columns:
        group_dvs = fit_HDDM(df,
                             categorical_dict = {'v': ['condition'],
                                                 'a': ['condition']},
                             **kwargs)
    else:
        group_dvs = fit_HDDM(df, **kwargs)
    return group_dvs

def threebytwo_HDDM(df, **kwargs):
    df = df.copy()

    df.loc[:,'cue_switch_binary'] = df.cue_switch.map(lambda x: ['cue_stay','cue_switch'][x!='stay'])
    df.loc[:,'task_switch_binary'] = df.task_switch.map(lambda x: ['task_stay','task_switch'][x!='stay'])
    group_dvs = fit_HDDM(df,
                         categorical_dict = {'v': ['cue_switch_binary', 'task_switch_binary', 'CTI']},
                         **kwargs)
    return group_dvs

def twobytwo_HDDM(df, **kwargs):
    df = df.copy()

    df.loc[:,'cue_switch_binary'] = df.cue_switch.map(lambda x: ['cue_stay','cue_switch'][x!='stay'])
    df.loc[:,'task_switch_binary'] = df.task_switch.map(lambda x: ['task_stay','task_switch'][x!='stay'])

    formula = "v ~ (C(cue_switch_binary, Sum)+C(task_switch_binary, Sum))*C(CTI,Sum) - C(CTI,Sum)"
    group_dvs = fit_HDDM(df,
                         categorical_dict = {'v': ['cue_switch_binary', 'task_switch_binary', 'CTI']},
                         formulas = formula,
                         **kwargs)
    return group_dvs


def get_HDDM_fun(task=None, kwargs=None):
    if kwargs is None:
        kwargs = {}
    if 'outfile' not in kwargs:
        kwargs['outfile']=task
    # remove unique kwargs
    mode = kwargs.pop('mode', 'proactive')
    hddm_fun_dict = \
    {
        'adaptive_n_back': lambda df: fit_HDDM(df.query('exp_stage == "adaptive"'),
                                               parametric_dict = {'v': ['load'],
                                                                  'a': ['load']},
                                               **kwargs),
        'attention_network_task': lambda df: ANT_HDDM(df, **kwargs),
        'choice_reaction_time': lambda df: fit_HDDM(df, **kwargs),
        'directed_forgetting': lambda df: directed_HDDM(df, **kwargs),
        'dot_pattern_expectancy': lambda df: DPX_HDDM(df, **kwargs),

        'local_global_letter': lambda df: fit_HDDM(df,
                                            categorical_dict = {'v': ['condition', 'conflict_condition', 'switch']},
                                            **kwargs),
        'motor_selective_stop_signal': lambda df: motor_SS_HDDM(df,
                                                                mode=mode,
                                                                **kwargs),
        'recent_probes': lambda df: recent_HDDM(df, **kwargs),
        'shape_matching': lambda df: shape_matching_HDDM(df, **kwargs),
        'simon': lambda df: fit_HDDM(df,
                                     categorical_dict = {'v': ['condition']},
                                     **kwargs),
        'stim_selective_stop_signal': lambda df: stim_SS_HDDM(df, **kwargs),
        'stop_signal': lambda df: SS_HDDM(df, **kwargs),
        'stroop': lambda df: fit_HDDM(df,
                                      categorical_dict = {'v': ['condition']},
                                      **kwargs),
        'threebytwo': lambda df: threebytwo_HDDM(df, **kwargs),
        'twobytwo': lambda df: twobytwo_HDDM(df, **kwargs)
    }
    if task is None:
        return hddm_fun_dict
    else:
        return hddm_fun_dict[task]
