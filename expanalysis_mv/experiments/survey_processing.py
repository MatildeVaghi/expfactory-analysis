"""
analysis/experiments/survey_processing.py: part of expfactory package
functions for automatically cleaning and manipulating surveys
"""
import numpy
import os
import pandas

# reference for calculating subscales
file_loc = os.path.dirname(os.path.realpath(__file__))
reference_scores = pandas.read_csv(os.path.join(file_loc,'survey_subscale_reference.csv'), index_col=0)

"""
Generic Functions
"""

def multi_worker_decorate(func):
    """Decorator to ensure that dv functions have only one worker
    """
    def multi_worker_wrap(group_df, use_check=True, survey_name=None):
        group_dvs = {}
        if len(group_df) == 0:
            return group_dvs, ''
        if 'passed_check' in group_df.columns and use_check:
            group_df = group_df[group_df['passed_check']]
        for worker in pandas.unique(group_df['worker_id']):
            df = group_df.query('worker_id == "%s"' %worker)
            try:
                if survey_name is None:
                    group_dvs[worker], description = func(df)
                else:
                    group_dvs[worker], description = func(df, survey_name)
            except:
                print('DV calculated failed for worker: %s' % worker)
        return group_dvs, description
    return multi_worker_wrap

def get_scores(survey):
    subset = reference_scores.filter(regex = survey, axis = 0)
    subscale_dict = {}
    for name, values in subset.iterrows():
        subscale_name = name.split('.')[1]
        subscale_items = [int(i) for i in values.tolist()[2:] if i == i]
        subscale_valence = values.iloc[1]
        subscale_dict[subscale_name] = [subscale_items, subscale_valence]
    return subscale_dict


def get_description(survey_name):
    mean = True
    if survey_name == 'bis_bas_survey':
        description = """
            Score for bias/bas. Higher values mean
            greater expression of that factor. BAS: "behavioral approach system",
            BIS: "Behavioral Inhibition System"
        """
    elif survey_name == 'brief_self_control_survey':
        description = """
            More self control-y
        """
        mean = False
    elif survey_name == 'dickman_survey':
        description = """
            Score for all dickman impulsivity survey. Higher values mean
            greater expression of that factor.
        """
    elif 'dospert' in survey_name:
        description = """
            Score for all dospert scales. Higher values mean
            greater expression of that factor.
        """
    elif survey_name == 'erq_survey':
        description = """
            Score for different emotion regulation strategies. Higher values mean
            greater expression of that strategy
        """
    elif survey_name == 'five_facet_mindfulness_survey':
        description = """
            Score for five factors mindfulness. Higher values mean
            greater expression of that value
        """
    elif survey_name == '^future_time_perspective_survey':
        description = """
            Future time perspective (FTP) level. Higher means being more attentive/
            influenced by future states
        """
        mean = False
    elif survey_name == 'grit_scale_survey':
        description = """
            Grit level. Higher means more gritty
        """
        mean = False
    elif survey_name == 'impulsive_venture_survey':
        description = """
            Score for i7. Higher values mean
            greater expression of that value. One question was removed from the original
            survey for venturesomeness: "Would you like to go pot-holing"
        """
    elif survey_name == 'mindful_attention_awareness_survey':
        description = """
            mindfulness level. Higher levels means higher levels of "dispositional mindfulness"
        """
    elif survey_name == 'mpq_control_survey':
        description = """
            control level. High scorers on this scale describe themselves as:
                Reflective; cautious, careful, plodding; rational,
                sensible, level-headed; liking to plan activities in detail.
        """
        mean = False
    elif survey_name == 'selection_optimization_compensation_survey':
        description = """
            Score for five different personality measures. Higher values mean
            greater expression of that personality
        """
    elif survey_name == 'selection_optimization_compensation_survey_correctlayout':
        description = """
            Score for five different personality measures. Higher values mean
            greater expression of that personality
        """
    elif survey_name == 'self_regulation_survey':
        description = """
            control level. High scorers means higher level of endorsement
        """
        mean = False
    elif survey_name == 'ten_item_personality_survey':
        description = """
            Score for five different personality measures. Higher values mean
            greater expression of that personality
        """
    elif survey_name == 'theories_of_willpower_survey':
        description = """
            Higher values on this survey indicate a greater endorsement of a
            "limited resource" theory of willpower
        """
        mean = False
    elif survey_name == '^time_perspective_survey':
        description = """
            Score for five different time perspective factors. High values indicate
            higher expression of that value
        """
    elif survey_name == 'upps_impulsivity_survey':
        description = """
            Score for five different upps+p measures. Higher values mean
            greater expression of that factor
        """
    elif survey_name == 'psychopathology_aes_survey__covid':
        description = """
            Higher values on this survey indicate a lower expression
            of apathy
        """
        mean = False
    elif survey_name == 'psychopathology_eat26_survey__covid':
        description = """
            Higher values on this survey (at or above 20) indicate a higher level of
            concern about dieting, body weight, or problematic eating behaviours.

        """
        mean = False
    elif survey_name == 'psychopathology_ocir_survey__covid':
        description = """
            Higher values on this survey indicate a higher expression
            of OCD traits. The possible range of scores is 0-72, mean score for
            peiple with OCD is 28.00, cut off score is 21
        """
        mean = False
    elif survey_name == 'psychopathology_sds_survey__covid':
        description = """
            Higher values on this survey indicate a higher expression
            of depression symptoms (20-44, normal range; 45-59, Mildly depressed,
            60-69, moderately depressed; 70 and above severely depressed)
        """
        mean = False
    elif survey_name == 'psychopathology_sms_survey__covid':
        description = """
            Higher values on this survey indicate a higher expression
            of schizotypy
        """
        mean = False
    elif survey_name == 'psychopathology_stai_survey__covid':
        description = """
            Higher values on this survey indicate a higher expression
            of anxiety. Scores range from 20 to 80.
        """
        mean = False
    elif survey_name == 'mindset_illness_stress_survey__covid':
        description = """
            This set of questions ask participant about their mindset towards
            illness and stress
        """
        mean = False
    return (description, mean)

"""
Demographics
"""

def get_response_text(data, qnum):
    text = numpy.nan
    if qnum in data.question_num.tolist():
        text = data[data.question_num == qnum].response_text[0]
        if text:
            try:
                text = text.strip()
            except:
                text=numpy.nan
    return text

def get_response_value(data,qnum, nan_values = []):
    value = numpy.nan
    if qnum in data.question_num.tolist():
        if not isinstance(nan_values,list):
            nan_values = [nan_values]
        try:
            value = int(data[data.question_num == qnum].response)
            if value in nan_values:
                value = numpy.nan
        except ValueError:
            pass
    return value

@multi_worker_decorate
def get_demographics_DV(df):
    dvs = {}
    dvs['age'] = {'value':  get_response_value(df, 3), 'valence': 'NA'}
    dvs['sex'] = {'value':  get_response_text(df, 2), 'valence': 'NA'}
    dvs['race'] = {'value':  list(df[df.question_num == 4].response), 'valence': 'NA'}
    dvs['hispanic?'] = {'value':  get_response_text(df, 6), 'valence': 'NA'}
    dvs['education'] = {'value':  get_response_text(df, 7), 'valence': 'Pos'}
    dvs['height(inches)'] = {'value':  get_response_value(df, 8), 'valence': 'NA'}
    dvs['weight(pounds)'] = {'value':  get_response_value(df, 9), 'valence': 'NA'}
    # calculate bmi
    weight_kilos = dvs['weight(pounds)']['value']*0.453592
    height_meters = dvs['height(inches)']['value']*.0254
    BMI = weight_kilos/height_meters**2
    if dvs['height(inches)']['value'] > 30 and dvs['weight(pounds)']['value'] > 50:
        dvs['BMI'] = {'value': BMI, 'valence': 'Neg'}
    else:
        dvs['BMI'] = {'value': numpy.nan, 'valence': 'Neg'}
    dvs['relationship_status'] = {'value':  get_response_text(df, 10), 'valence': 'NA'}
    dvs['divoce_count'] = {'value':  get_response_text(df, 11), 'valence': 'NA'}
    dvs['longest_relationship(months)'] = {'value':  get_response_value(df, 12), 'valence': 'NA'}
    dvs['relationship_count'] = {'value':  get_response_text(df, 13), 'valence': 'NA'}
    dvs['children_count'] = {'value':  get_response_text(df, 14), 'valence': 'NA'}
    dvs['household_income(dollars)'] = {'value':  get_response_value(df, 15), 'valence': 'NA'}
    dvs['retirement_account?'] = {'value':  get_response_text(df, 16), 'valence': 'NA'}
    dvs['percent_retirement_in_stock'] = {'value':  get_response_text(df, 17), 'valence': 'NA'}
    dvs['home_status'] = {'value':  get_response_text(df, 18), 'valence': 'NA'}
    dvs['mortage_debt'] = {'value':  get_response_text(df, 19), 'valence': 'NA'}
    dvs['car_debt'] = {'value':  get_response_text(df, 20), 'valence': 'NA'}
    dvs['education_debt'] = {'value':  get_response_text(df, 21), 'valence': 'NA'}
    dvs['credit_card_debt'] = {'value':  get_response_text(df, 22), 'valence': 'NA'}
    dvs['other_sources_of_debt'] = {'value':  get_response_text(df, 24), 'valence': 'NA'}
    #calculate total caffeine intake
    caffeine_intake = \
        get_response_value(df, 25)*100 + \
        get_response_value(df, 26)*40 + \
        get_response_value(df, 27)*30 + \
        get_response_value(df, 28)
    dvs['caffeine_intake'] = {'value':  caffeine_intake, 'valence': 'NA'}
    dvs['gambling_problem?'] = {'value':  get_response_text(df, 29), 'valence': 'Neg'}
    dvs['traffic_ticket_count'] = {'value':  get_response_text(df, 30), 'valence': 'Neg'}
    dvs['traffic_accident_count'] = {'value':  get_response_text(df, 31), 'valence': 'Neg'}
    dvs['arrest_count'] = {'value':  get_response_text(df, 32), 'valence': 'Neg'}
    dvs['mturk_motivation'] = {'value':  list(df[df.question_num == 33].response), 'valence': 'NA'}
    dvs['other_motivation'] = {'value':  get_response_text(df, 34), 'valence': 'NA'}
    description = "Outputs various demographic variables"
    return dvs,description



@multi_worker_decorate
def calc_demographics_DV(df):
    dvs = {}
    dvs['age'] = {'value': get_response_value(df,3), 'valence': 'NA'}
    dvs['sex'] = {'value':  get_response_text(df,2), 'valence': 'NA'}
    dvs['race'] = {'value':  list(df[df.question_num == 4].response), 'valence': 'NA'}
    dvs['hispanic?'] = {'value':  get_response_text(df,6), 'valence': 'NA'}
    dvs['education'] = {'value':  get_response_value(df,7), 'valence': 'Pos'}
    dvs['height(inches)'] = {'value':  get_response_value(df,8), 'valence': 'NA'}
    dvs['weight(pounds)'] = {'value':  get_response_value(df,9), 'valence': 'NA'}
    # calculate bmi
    weight_kilos = dvs['weight(pounds)']['value']*0.453592
    height_meters = dvs['height(inches)']['value']*.0254
    BMI = weight_kilos/height_meters**2
    if dvs['height(inches)']['value'] > 30 and dvs['weight(pounds)']['value'] > 50:
        dvs['BMI'] = {'value': BMI, 'valence': 'Neg'}
    else:
        dvs['BMI'] = {'value': numpy.nan, 'valence': 'Neg'}
    dvs['relationship_status'] = {'value':  get_response_text(df,10), 'valence': 'NA'}
    dvs['divoce_count'] = {'value':  get_response_value(df,11), 'valence': 'NA'}
    dvs['longest_relationship(months)'] = {'value':  get_response_value(df,12), 'valence': 'NA'}
    dvs['relationship_count'] = {'value':  get_response_value(df,13), 'valence': 'NA'}
    dvs['children_count'] = {'value':  get_response_value(df,14), 'valence': 'NA'}
    dvs['household_income(dollars)'] = {'value':  get_response_value(df,15), 'valence': 'NA'}
    dvs['retirement_account?'] = {'value':  get_response_text(df,16), 'valence': 'NA'}
    dvs['percent_retirement_in_stock'] = {'value':  get_response_value(df,17), 'valence': 'NA'}
    dvs['home_status'] = {'value':  get_response_text(df,18), 'valence': 'NA'}
    dvs['mortage_debt'] = {'value':  get_response_value(df,19,nan_values = 0), 'valence': 'NA'}
    dvs['car_debt'] = {'value':  get_response_value(df,20,nan_values = 0), 'valence': 'NA'}
    dvs['education_debt'] = {'value':  get_response_value(df,21,nan_values = 0), 'valence': 'NA'}
    dvs['credit_card_debt'] = {'value':  get_response_value(df,22,nan_values = 0), 'valence': 'NA'}
    dvs['other_sources_of_debt'] = {'value':  get_response_value(df,24,nan_values = 0), 'valence': 'NA'}
    #calculate total caffeine intake
    caffeine_intake = \
        get_response_value(df,25)*100 + \
        get_response_value(df,26)*40 + \
        get_response_value(df,27)*30 + \
        get_response_value(df,28)
    dvs['caffeine_intake'] = {'value':  caffeine_intake, 'valence': 'NA'}
    dvs['gambling_problem?'] = {'value':  get_response_text(df,29), 'valence': 'Neg'}
    dvs['traffic_ticket_count'] = {'value':  get_response_value(df,30), 'valence': 'Neg'}
    dvs['traffic_accident_count'] = {'value':  get_response_value(df,31), 'valence': 'Neg'}
    dvs['arrest_count'] = {'value':  get_response_value(df,32), 'valence': 'Neg'}
    dvs['mturk_motivation'] = {'value':  list(df[df.question_num == 33].response), 'valence': 'NA'}
    dvs['other_motivation'] = {'value':  df[df.question_num == 34].response.iloc[0] or numpy.nan, 'valence': 'NA'}
    description = "Outputs various demographic variables"
    return dvs,description

@multi_worker_decorate
def calc_demographics_covid_DV(df):
    dvs = {}
    dvs['sex'] = {'value':  get_response_text(df, 2), 'valence': 'NA'}
    dvs['age'] = {'value':  get_response_value(df, 3), 'valence': 'NA'}
    dvs['race'] = {'value':  list(df[df.question_num == 4].response), 'valence': 'NA'}
    dvs['hispanic?'] = {'value':  get_response_text(df, 6), 'valence': 'NA'}
    dvs['education'] = {'value':  get_response_text(df, 7), 'valence': 'Pos'}
    dvs['height(feet)'] = {'value':  get_response_value(df, 9), 'valence': 'NA'}
    dvs['height(inches)'] = {'value':  get_response_value(df, 10), 'valence': 'NA'}
    dvs['height(tot_inches)'] = {'value': dvs['height(inches)'] ['value'] + dvs['height(feet)']['value']*12, 'valence': 'NA'}
    dvs['weight(pounds)'] = {'value':  get_response_value(df, 11), 'valence': 'NA'}
    #compute BMI
    weight_kilos = dvs['weight(pounds)']['value']*0.453592
    height_meters =dvs['height(tot_inches)']['value']*.0254
    BMI = weight_kilos/height_meters**2
    if dvs['height(tot_inches)']['value'] > 30 and dvs['weight(pounds)']['value'] > 50:
        dvs['BMI'] = {'value': BMI, 'valence': 'Neg'}
    else:
        dvs['BMI'] = {'value': numpy.nan, 'valence': 'Neg'}
    dvs['relationship_status'] = {'value':  get_response_text(df, 12), 'valence': 'NA'}
    dvs['divorce_count'] = {'value':  get_response_text(df, 13), 'valence': 'NA'}
    dvs['longest_relationship(years)'] = {'value':  get_response_value(df, 15), 'valence': 'NA'}
    dvs['longest_relationship(months)'] = {'value':  get_response_value(df, 16), 'valence': 'NA'}
    dvs['relationship_count'] = {'value':  get_response_text(df, 17), 'valence': 'NA'}
    dvs['children_count'] = {'value':  get_response_text(df, 18), 'valence': 'NA'}
    dvs['household_income(dollars)'] = {'value':  get_response_value(df, 19), 'valence': 'NA'}
    dvs['retirement_account?'] = {'value':  get_response_text(df, 20), 'valence': 'NA'}
    dvs['percent_retirement_in_stock'] = {'value':  get_response_text(df, 21), 'valence': 'NA'}
    dvs['home_status'] = {'value':  get_response_text(df, 22), 'valence': 'NA'}
    dvs['mortage_debt'] = {'value':  get_response_text(df, 23), 'valence': 'NA'}
    dvs['car_debt'] = {'value':  get_response_text(df, 24), 'valence': 'NA'}
    dvs['education_debt'] = {'value':  get_response_text(df, 25), 'valence': 'NA'}
    dvs['credit_card_debt'] = {'value':  get_response_text(df, 26), 'valence': 'NA'}
    dvs['other_sources_of_debt'] = {'value':  get_response_text(df, 27), 'valence': 'NA'}
    #calculate total caffeine intake
    caffeine_intake = \
        get_response_value(df, 29)*100 + \
        get_response_value(df, 30)*40 + \
        get_response_value(df, 31)*30 + \
        get_response_value(df, 32)
    dvs['caffeine_intake'] = {'value':  caffeine_intake, 'valence': 'NA'}
    dvs['gambling_problem?'] = {'value':  get_response_text(df, 33), 'valence': 'Neg'}
    dvs['traffic_ticket_count'] = {'value':  get_response_text(df, 34), 'valence': 'Neg'}
    dvs['traffic_accident_count'] = {'value':  get_response_text(df, 35), 'valence': 'Neg'}
    dvs['arrest_count'] = {'value':  get_response_text(df, 36), 'valence': 'Neg'}
    dvs['mturk_motivation'] = {'value':  list(df[df.question_num == 37].response), 'valence': 'NA'}
    dvs['other_motivation'] = {'value':  get_response_text(df, 38), 'valence': 'NA'}
    dvs['state'] = {'value':  get_response_text(df, 40), 'valence': 'NA'}
    dvs['other_state'] = {'value':  get_response_text(df, 41), 'valence': 'NA'}
    dvs['crisis_currentlyworking'] = {'value':  get_response_text(df, 42), 'valence': 'NA'}
    dvs['crisis_occupation'] = {'value':  list(df[df.question_num == 43].response), 'valence': 'NA'}
    dvs['crisis_military'] = {'value':  get_response_text(df, 44), 'valence': 'NA'}
    dvs['crisis_urbanicity'] = {'value':  get_response_text(df, 45), 'valence': 'NA'}
    dvs['crisis_householdpeople'] = {'value':  int(df[df.question_num == 46].response), 'valence': 'NA'}
    dvs['crisis_householdrelation'] = {'value':  list(df[df.question_num == 47].response), 'valence': 'NA'}
    dvs['crisis_esssentialworkers'] = {'value':  get_response_text(df, 48), 'valence': 'NA'}
    dvs['crisis_workershome'] = {'value':  get_response_text(df, 50), 'valence': 'NA'}
    dvs['crisis_workersforcovid'] = {'value':  get_response_text(df, 51), 'valence': 'NA'}
    dvs['crisis_householdrooms'] = {'value':  int(df[df.question_num == 52].response), 'valence': 'NA'}
    dvs['crisis_healthinsurance'] = {'value':  get_response_text(df, 53), 'valence': 'NA'}
    dvs['crisis_moneysupport'] = {'value':  get_response_text(df, 54), 'valence': 'NA'}
    dvs['crisis_physicalhealth'] = {'value':  get_response_text(df, 55), 'valence': 'NA'}
    dvs['crisis_mentalhealth'] = {'value':  get_response_text(df, 56), 'valence': 'NA'}
    description = "Outputs various demographic variables, including some from CRISIS questionnairs"
    return dvs,description


"""
Post Processing functions
"""
def future_time_post(df):
    df = df.copy()
    # future time perspective was bugged before 2019. Correc those
    bugged_subset = df[df['finishtime']<'2019']
    bugged_subset = bugged_subset.query('question_num >=9')
    fixed = 8-bugged_subset['response']
    df.loc[fixed.index, 'response'] = fixed
    return df

def self_regulation_survey_post(df):
    def abs_diff(lst):
        if len(lst)==2:
            return abs(lst[0]-lst[1])
        else:
            return numpy.nan
    df.response = df.response.astype(float)
    avg_response = df.query('question_num in %s' %[24,26]).groupby('worker_id').response.mean().tolist()
    abs_diff = df.query('question_num in %s' %[24,26]).groupby('worker_id').response.agg(abs_diff)
    df.loc[df.question_num == 26,'response'] = avg_response
    df.loc[df.question_num == 26,'repeat_response_diff'] = abs_diff
    df = df.query('question_num != 24')
    return df

def sensation_seeking_survey_post(df):
    # remove item 10 if it contains an error. The second option should be
    # "I'd never smoke marijuana" not "I would like to try some of the new drugs that produce hallucinations"
    bugged_index = df.query('question_num==10').index
    if len(bugged_index) > 0:
        if type(df.loc[bugged_index[0], "options"]) == str:
            potential_bugged_question = [i['text'] for i in eval(df.loc[bugged_index[0], "options"])]
        else:
            potential_bugged_question = [i['text'] for i in df.loc[bugged_index[0], "options"]]
        if "I would never smoke marijuana" not in potential_bugged_question:
            df = df.drop(bugged_index)
    return df

def sensation_seeking_survey_correctlayout_post(df):
    # in the new layout it is item 11 if it contains an error. The second option should be
    # "I'd never smoke marijuana" not "I would like to try some of the new drugs that produce hallucinations"
    bugged_index = df.query('question_num==11').index
    if len(bugged_index) > 0:
        if type(df.loc[bugged_index[0], "options"]) == str:
            potential_bugged_question = [i['text'] for i in eval(df.loc[bugged_index[0], "options"])]
        else:
            potential_bugged_question= df.loc[bugged_index[0], "text"]
        if "I would never smoke marijuana" not in potential_bugged_question:
            df = df.drop(bugged_index)
    return df


"""
DV functions
"""
@multi_worker_decorate
def calc_survey_DV(df, survey_name):
    df.insert(0,'numeric_response', df['response'].astype(float))
    scores = get_scores(survey_name)
    DVs = {}
    description, mean = get_description(survey_name)
    for score,subset in scores.items():
        score_subset = df.query('question_num in %s' % subset[0]).numeric_response
        if len(subset[0]) == len(score_subset):
            if mean==True:
                DVs[score] = {'value': score_subset.mean(), 'valence': subset[1]}
            else:
                DVs[score] = {'value': score_subset.sum(), 'valence': subset[1]}
        else:
            print("%s score couldn't be calculated for subject %s" % (score, df.worker_id.unique()[0]))
    return DVs,description


@multi_worker_decorate
def calc_bis11_DV(df):
    df.insert(0,'numeric_response', df['response'].astype(float))
    scores = get_scores('bis11_survey.first')
    scores.update(get_scores('bis11_survey.total'))
    DVs = {}
    for score,subset in scores.items():
        score_subset = df.query('question_num in %s' % subset[0]).numeric_response
        if len(subset[0]) == len(score_subset):
            DVs[score] = {'value': score_subset.mean(), 'valence': subset[1]}
        else:
            print("%s score couldn't be calculated for subject %s" % (score, df.worker_id.unique()[0]))
    DVs['Attentional'] = {'value':  DVs['first_order_attention']['value'] + DVs['first_order_cognitive_stability']['value'], 'valence': 'Neg'}
    DVs['Motor'] = {'value':  DVs ['first_order_motor']['value'] + DVs['first_order_perseverance']['value'], 'valence': 'Neg'}
    DVs['Nonplanning'] = {'value':  DVs['first_order_self_control']['value'] + DVs['first_order_cognitive_complexity']['value'], 'valence': 'Neg'}
    description = """
        Score for bis11. Higher values mean
        greater expression of that "impulsive" factor. High values are negative traits. "Attentional", "Motor" and "Nonplanning"
        are second-order factors, while the other 6 are first order factors.
    """
    return DVs,description

@multi_worker_decorate
def calc_eating_DV(df):
    """
    Scores are normalized
    Reference: Lauzon et al., 2004, Journal of Nutrition
    """
    df.insert(0,'numeric_response', df['response'].astype(float))
    scores = get_scores('eating')
    DVs = {}
    for score,subset in scores.items():
        score_subset = df.query('question_num in %s' % subset[0]).numeric_response
        if len(subset[0]) == len(score_subset):
            raw_score = df.query('question_num in %s' % subset[0]).numeric_response.sum()
            min_raw = len(subset[0])
            raw_range = min_raw*3 # max = min_raw*4
            normalized_score = (raw_score-min_raw)/raw_range*100
            DVs[score] = {'value': normalized_score, 'valence': subset[1]}
        else:
            print("%s score couldn't be calculated for subject %s" % (score, df.worker_id.unique()[0]))
    DVs['total'] = {'value': df['numeric_response'].sum(), 'valence': 'Pos'}
    description = """
        Score for three eating components. Higher values mean
        greater expression of that value
    """
    return DVs,description

@multi_worker_decorate
def calc_leisure_time_DV(df):
    DVs = {'activity_level': {'value': float(df.iloc[0]['response']), 'valence': 'Pos'}}
    description = """
        Exercise level. Higher means more exercise
    """
    return DVs,description


@multi_worker_decorate
def calc_lon_socsup_pss_post_DV(df):
    df.insert(0,'numeric_response', df['response'].astype(float))
    scores = get_scores('loneliness_socialsupport_stress_post_survey__covid')
    DVs = {}
    for score,subset in scores.items():
        score_subset = df.query('question_num in %s' % subset[0]).numeric_response
        if len(subset[0]) == len(score_subset):
            if 'socsupp' in score:
                DVs[score] = {'value': score_subset.mean(), 'valence': subset[1]}
            else:
                DVs[score] = {'value': score_subset.sum(), 'valence': subset[1]}
        else:
            print("%s score couldn't be calculated for subject %s" % (score, df.worker_id.unique()[0]))
        description = """
            This survey includes 1) the short scale for measuring loneliness in large
            surveys (higher values on this scale indicate higher levels of
            loneliness); 2) the perceived stress scale (higher values indicate Higher
            levels of perceived stress); 3) the multidimensional scale
            of perceived social support (higher values indicates higher values of
            social support). Scores on this set of questions
            refer to the period post covid
        """
    return DVs,description

@multi_worker_decorate
def calc_lon_socsup_pss_pre_DV(df):
    df.insert(0,'numeric_response', df['response'].astype(float))
    scores = get_scores('loneliness_socialsupport_stress_pre_survey__covid')
    DVs = {}
    for score,subset in scores.items():
        score_subset = df.query('question_num in %s' % subset[0]).numeric_response
        if len(subset[0]) == len(score_subset):
            if 'socsupp' in score:
                DVs[score] = {'value': score_subset.mean(), 'valence': subset[1]}
            else:
                DVs[score] = {'value': score_subset.sum(), 'valence': subset[1]}
        else:
            print("%s score couldn't be calculated for subject %s" % (score, df.worker_id.unique()[0]))
        description = """
            This survey includes 1) the short scale for measuring loneliness in large
            surveys (higher values on this scale indicate higher levels of
            loneliness); 2) the perceived stress scale (higher values indicate Higher
            levels of perceived stress); 3) the multidimensional scale
            of perceived social support (higher values indicates higher values of
            social support). Scores on this set of questions
            refer to the period pre covid
        """
    return DVs,description


@multi_worker_decorate
def calc_lsas_DV(df):
    df.insert(0,'numeric_response', df['response'].astype(float))
    scores = get_scores('psychopathology_lsas_survey__covid')
    DVs = {}
    nitems_avoidance = len(df[df['text'].str.contains("Avoidance")])
    nitems_fear     =  len(df[df['text'].str.contains("Fear")])
    if nitems_avoidance == nitems_fear:
        scale_score = {}
        for i in range(0, int((len(scores['avoidance'][0]) + len(scores['fear'][0]))/ 2)):
            response_fear   = (scores['fear'][0][i])
            response_avoidance = (scores['avoidance'][0][i])
            #Value for each item is the average of the values for the same question
            #in relation to fear and avoidance, see also Rouault et al., 2018
            item_score = df.query('question_num in %s' %[response_fear,response_avoidance]).numeric_response.mean().tolist()
            scale_score[i] = item_score
    else:
        print("%s score couldn't be calculated for subject %s" % (score, df.worker_id.unique()[0]))
    DVs['psych_lsas_total'] = {'value': sum([scale_score[key] for key in scale_score]), 'valence': 'NA'}
    description = """
        Score for lsas. Higher values mean
        greater expression of social anxiety.
    """
    return DVs,description


@multi_worker_decorate
def calc_SSS_DV(df):
    df.insert(0,'numeric_response', df['response'].astype(float))
    scores = get_scores('sensation_seeking_survey')
    DVs = {}
    for score,subset in scores.items():
        score_subset = df.query('question_num in %s' % subset[0]).numeric_response
        if len(subset[0]) == len(score_subset):
            DVs[score] = {'value': score_subset.mean(), 'valence': subset[1]}
        # allow for bugged survey such that the 10th OR 11th depending on layout item is omitted - see post processing
        elif (score=='experience_seeking') and (len(subset[0]) == len(score_subset)+1):
            DVs[score] = {'value': score_subset.mean(), 'valence': subset[1]}
        else:
            print("%s score couldn't be calculated for subject %s" % (score, df.worker_id.unique()[0]))
    DVs['total'] = {'value': df['numeric_response'].sum(), 'valence': 'Pos'}
    description = """
        Score for SSS-V. Higher values mean
        greater expression of that trait
    """
    return DVs,description


@multi_worker_decorate
def calc_SSS_corrlayout_DV(df):
    df.insert(0,'numeric_response', df['response'].astype(float))
    scores = get_scores('sensation_seeking_survey_correctlayout')
    DVs = {}
    for score,subset in scores.items():
        score_subset = df.query('question_num in %s' % subset[0]).numeric_response
        if len(subset[0]) == len(score_subset):
            DVs[score] = {'value': score_subset.mean(), 'valence': subset[1]}
        # allow for bugged survey such that the 10th OR 11th depending on layout item is omitted - see post processing
        elif (score=='experience_seeking') and (len(subset[0]) == len(score_subset)+1):
            DVs[score] = {'value': score_subset.mean(), 'valence': subset[1]}
        else:
            print("%s score couldn't be calculated for subject %s" % (score, df.worker_id.unique()[0]))
    DVs['total'] = {'value': df['numeric_response'].sum(), 'valence': 'Pos'}
    description = """
        Score for SSS-V. Higher values mean
        greater expression of that trait
    """
    return DVs,description
