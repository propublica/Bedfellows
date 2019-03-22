from peewee import *
from csv import DictReader
from datetime import datetime
import os
import pdb
from tqdm import tqdm

# Do this in a settings file
database = MySQLDatabase(
    'fec',
    user='root',
    password='',
    host='localhost'
)


class BaseModel(Model):
    @classmethod
    def create_tables(cls):
        database.create_tables([cls])

    class Meta:
        legacy_table_names = False
        database = database


class FecCommitteeContributions(BaseModel):
    fec_committee_id = CharField(null=True, max_length=10)
    amendment = CharField(null=True)
    report_type = CharField(null=True)
    pgi = CharField(null=True)
    microfilm = CharField(null=True)
    transaction_type = CharField(null=True)
    entity_type = CharField(null=True)
    contributor_name = CharField(null=True)
    city = CharField(null=True)
    state = CharField(null=True, max_length=2)
    zipcode =  CharField(null=True, max_length=10)
    employer = CharField(null=True)
    occupation = CharField(null=True)
    date = DateTimeField(null=True)
    amount = IntegerField(null=True)
    other_id = CharField(null=True, max_length=10)
    recipient_name = CharField(null=True)
    recipient_state = CharField(null=True, max_length=2)
    recipient_party = CharField(null=True)
    cycle = CharField(null=True)
    transaction_id = CharField(null=True)
    filing_id = CharField(null=True)
    memo_code = CharField(null=True)
    memo_text = CharField(null=True)
    fec_record_number = CharField(null=True)

    @classmethod
    def load(cls, path):
        csv_in = os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            path
        )
        with open(csv_in) as csvfile, database.atomic():
            rows = DictReader(csvfile)
            print('read in csv')
            for batch in tqdm(chunked(rows, 100)):
                src_data = []
                for row in batch:
                    if row['date'] != '':
                        row['date'] = datetime.strptime(row['date'], '%m/%d/%Y')
                    else:
                        row['date'] = None
                    row['amount'] = int(row['amount'])
                    src_data.append(row)

                cls.insert_many(src_data).execute()

FecCommitteeContributions.add_index(
    FecCommitteeContributions.transaction_type,
    FecCommitteeContributions.entity_type,
    FecCommitteeContributions.date,
    FecCommitteeContributions.fec_committee_id,
    FecCommitteeContributions.other_id,
    name='combo'
)


class FecCommittees(BaseModel):
    fecid = CharField(null=True, index=True, max_length=10)
    name = CharField(null=True)
    treasurer = CharField(null=True)
    address_one = CharField(null=True)
    address_two = CharField(null=True)
    city = CharField(null=True)
    state = CharField(null=True, max_length=2)
    zip = CharField(null=True, max_length=10)
    designation = CharField(null=True)
    committee_type = CharField(null=True, index=True)
    party = CharField(null=True, max_length=3)
    filing_frequency = CharField(null=True)
    interest_group = CharField(null=True)
    organization = CharField(null=True)
    fec_candidate_id = CharField(null=True, max_length=10)
    cycle = CharField(null=True, index=True, max_length=5)
    is_leadership = BooleanField(default=False)
    is_super_pac = BooleanField(default=False, index=True)

    @classmethod
    def load(cls, path):
        csv_in = os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            path
        )
        src_data = []
        with open(csv_in) as csvfile:
            rows = DictReader(csvfile)
            for batch in chunked(rows, 100):
                src_data = []
                for row in batch:
                    row['is_leadership'] = True if row['is_leadership'] == 't' else False
                    row['is_super_pac'] = True if row['is_super_pac'] == 't' else False
                    src_data.append(row)
                cls.insert_many(src_data).execute()

FecCommittees.add_index(
    FecCommittees.fec_candidate_id,
    FecCommittees.fecid,
    name='pair'
)


class FecCandidates(BaseModel):
    fecid = CharField(null=True, max_length=10)
    name = CharField(null=True)
    party = CharField(null=True, max_length=3)
    status = CharField(null=True)
    address_one = CharField(null=True)
    address_two = CharField(null=True)
    city = CharField(null=True)
    state = CharField(null=True, max_length=2)
    zip = CharField(null=True, max_length=10)
    fec_committee_id = CharField(null=True, max_length=10)
    cycle = CharField(null=True, max_length=5)
    district = CharField(null=True, max_length=4)
    office_state = CharField(null=True, max_length=2)
    cand_status = CharField(null=True)
    branch = CharField(null=True, max_length=1)

    @classmethod
    def load(cls, path):
        csv_in = os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            path
        )
        with open(csv_in) as csvfile:
            rows = DictReader(csvfile)
            src_data = list(rows)
        for batch in tqdm(chunked(src_data, 100)):
            cls.insert_many(batch).execute()

FecCandidates.add_index(
    FecCandidates.fecid,
    FecCandidates.name,
    FecCandidates.district,
    FecCandidates.office_state,
    FecCandidates.branch,
    FecCandidates.cycle,
    name='combo',
)


class FecContributions(BaseModel):
    fec_committee_id = CharField(null=True, max_length=10)
    report_type = CharField(null=True)
    contributor_name = CharField(null=True)
    date = DateTimeField(null=True)
    amount = CharField(null=True)
    other_id = CharField(null=True, index=True, max_length=10)
    recipient_name = CharField(null=True)
    cycle = CharField(null=True, max_length=5)

    # Move this method
    @classmethod
    def load(cls):
        keys = cls._meta.sorted_field_names
        keys.remove(cls._meta.primary_key.name)

        from_fields = [v for k,v in FecCommitteeContributions._meta.fields.items() if k in keys]
        to_fields = [v for k,v in cls._meta.fields.items() if k in keys]

        super_pac_query = FecCommittees\
            .select(FecCommittees.fecid)\
            .where(FecCommittees.is_super_pac == True)

        super_pac_ids = [c['fecid'] for c in super_pac_query.dicts()]

        cls.insert_from(
            FecCommitteeContributions\
                .select(*from_fields)\
                .where(FecCommitteeContributions.fec_committee_id.not_in(super_pac_ids))\
                .where(FecCommitteeContributions.other_id.not_in(super_pac_ids)),
            fields=to_fields
        ).execute()

FecContributions.add_index(
    FecContributions.fec_committee_id,
    FecContributions.other_id,
    FecContributions.report_type,
    name='pair_report_type'
)

FecContributions.add_index(
    FecContributions.cycle,
    FecContributions.fec_committee_id,
    FecContributions.other_id,
    name='cycle_pair'
)

FecContributions.add_index(
    FecContributions.fec_committee_id,
    FecContributions.cycle,
    FecContributions.other_id,
    FecContributions.contributor_name,
    FecContributions.recipient_name,
    FecContributions.date,
    FecContributions.amount,
    name='pair_cycle_date'
)


class TotalDonatedByContributor(BaseModel):
    fec_committee_id = CharField(null=True, max_length=9)
    contributor_name = CharField(null=True, max_length=200)
    total_by_PAC = FloatField(null=True, )

TotalDonatedByContributor.add_index(
    TotalDonatedByContributor.fec_committee_id,
    TotalDonatedByContributor.contributor_name,
    TotalDonatedByContributor.total_by_PAC,
    name='combo'
)


class ExclusivityScores(BaseModel):
    fec_committee_id = CharField(null=True, max_length=9)
    contributor_name = CharField(null=True, max_length=200)
    total_by_pac = CharField(null=True, max_length=10)
    other_id = CharField(null=True, max_length=9)
    recipient_name = CharField(null=True, max_length=200)
    amount = CharField(null=True, max_length=10)

ExclusivityScores.add_index(
    ExclusivityScores.fec_committee_id,
    ExclusivityScores.other_id,
    ExclusivityScores.contributor_name,
    name="pairs"
)


class ReportTypeWeights(BaseModel):
    report_type = CharField(null=True, max_length=5)
    year_parity = CharField(null=True, max_length=5)
    weight = IntegerField()

ReportTypeWeights.add_index(
    ReportTypeWeights.report_type,
    ReportTypeWeights.year_parity,
    ReportTypeWeights.weight,
    name='weights'
)


class ReportTypeCountByPair(BaseModel):
    fec_committee_id = CharField(null=True, max_length=9)
    contributor_name = CharField(max_length=200)
    other_id = CharField(max_length=9, null=False)
    recipient_name = CharField(max_length=200)
    report_type = CharField(max_length=4)
    year_parity = CharField(max_length=5)
    d_date = DateTimeField()
    count = IntegerField()

    class Meta:
        indexes = (
            (('fec_committee_id', 'other_id'), False),
        )


class PairsCount(BaseModel):
    fec_committee_id = CharField(max_length=9, null=False)
    other_id = CharField(max_length=9, null=False)
    count = IntegerField()

    class Meta:
        indexes = (
            (('fec_committee_id', 'other_id', 'count'), False),
        )


class ReportTypeFrequency(BaseModel):
    fec_committee_id = CharField(max_length=9, null=False)
    contributor_name = CharField(max_length=200)
    other_id = CharField(max_length=9, null=False)
    recipient_name = CharField(max_length=200)
    report_type = CharField(max_length=4)
    year_parity = CharField(max_length=5)
    d_date = DateTimeField()
    report_type_count_by_pair = CharField(max_length=10)
    pairs_count = IntegerField()
    report_type_frequency = FloatField()

    class Meta:
        indexes = (
            (('report_type',
            'year_parity',
            'fec_committee_id',
            'contributor_name',
            'other_id',
            'recipient_name',
            'report_type_frequency'),
            False)
        )


class UnnormalizedReportTypeScores(BaseModel):
    fec_committee_id = CharField(max_length=9, null=False)
    contributor_name = CharField(max_length=200)
    other_id = CharField(max_length=9, null=False)
    recipient_name = CharField(max_length=200)
    report_type_score = FloatField()

    class Meta:
        indexes = (
            (('fec_committee_id',
            'contributor_name',
            'other_id',
            'recipient_name',
            'report_type_score'),
            False)
        )


class MaxReportTypeScore(BaseModel):
    max_report_type_score = FloatField()


class ReportTypeScores(UnnormalizedReportTypeScores):
    pass

ReportTypeScores.add_index(
    ReportTypeScores.fec_committee_id,
    ReportTypeScores.other_id
)

class UnnormalizedPeriodicityScores(BaseModel):
    fec_committee_id = CharField(max_length=9, null=False)
    contributor_name = CharField(max_length=200)
    other_id = CharField(max_length=9, null=False)
    recipient_name = CharField(max_length=200)
    stddev_pop = FloatField()
    day_diff = IntegerField()
    periodicity_score = FloatField()

    class Meta:
        indexes = (
            (('fec_committee_id',
            'contributor_name',
            'other_id',
            'recipient_name',
            'periodicity_score'),
            False),
        )


class CapUnnormalizedScore(BaseModel):
    cap_unnormalized_score = FloatField()


class PeriodicityScores(BaseModel):
    fec_committee_id = CharField(max_length=9, null=False)
    contributor_name = CharField(max_length=200)
    other_id = CharField(max_length=9, null=False)
    recipient_name = CharField(max_length=200)
    periodicity_score = FloatField()

    class Meta:
        indexes = (
            (('fec_committee_id', 'other_id'), False),
        )


class ContributorTypes(BaseModel):
    fec_committee_id = CharField(max_length=9, null=False)
    contributor_name = CharField(max_length=200)
    cycle = CharField(max_length=5)
    contributor_type = CharField(max_length=15)

    class Meta:
        indexes = (
            (('fec_committee_id', 'cycle'), False)
        )


class RecipientTypes(BaseModel):
    other_id = CharField(max_length=9, null=False)
    recipient_name = CharField(max_length=200)
    cycle = CharField(max_length=5)
    recipient_type = CharField(max_length=15)


class ContributionLimits(BaseModel):
    contributor_type = CharField(max_length=15, null=False)
    recipient_type = CharField(max_length=15, null=False)
    cycle = CharField(max_length=5)
    contribution_limit = FloatField()

    class Meta:
        indexes = (
            (('contributor_type',
            'recipient_type',
            'cycle',
            'contribution_limit'),
            False),
        )


class JoinedContrRecptTypes(BaseModel):
    fec_committee_id = CharField(max_length=9, null=False)
    contributor_name = CharField(max_length=200)
    contributor_type = CharField(max_length=15)
    other_id = CharField(max_length=9, null=False)
    recipient_name = CharField(max_length=200)
    recipient_type = CharField(max_length=15)
    cycle = CharField(max_length=5)
    date = DateTimeField()
    amount = FloatField(20)

    class Meta:
        indexes = (
            (('contributor_type', 'recipient_type', 'cycle'),
            False)
        )


class MaxedOutSubscores(BaseModel):
    fec_committee_id = CharField(max_length=9, null=False)
    contributor_name = CharField(max_length=200)
    contributor_type = CharField(max_length=15)
    other_id = CharField(max_length=9, null=False)
    recipient_name = CharField(max_length=200)
    recipient_type = CharField(max_length=15)
    cycle = CharField(max_length=5)
    date = DateField()
    amount = FloatField()
    contribution_limit = FloatField()
    maxed_out_subscore = FloatField()

    class Meta:
        indexes = (
            (('fec_committee_id', 'other_id', 'cycle'), False),
        )


class InboundMaxedOutSubscores(MaxedOutSubscores):
    pass


class UnnormalizedMaxedOutScores(BaseModel):
    fec_committee_id = CharField(max_length=9, null=False)
    contributor_name = CharField(max_length=200)
    contributor_type = CharField(max_length=15)
    other_id = CharField(max_length=9, null=False)
    recipient_name = CharField(max_length=200)
    recipient_type = CharField(max_length=18)
    maxed_out_score = FloatField()

    class Meta:
        indexes = (
            (('fec_committee_id', 'contributor_name', 'contributor_type', 'other_id', 'recipient_name', 'recipient_type', 'maxed_out_score'),
            False)
        )

UnnormalizedMaxedOutScores.add_index(
    UnnormalizedMaxedOutScores.fec_committee_id,
    UnnormalizedMaxedOutScores.contributor_name,
    UnnormalizedMaxedOutScores.contributor_type,
    UnnormalizedMaxedOutScores.other_id,
    UnnormalizedMaxedOutScores.recipient_name,
    UnnormalizedMaxedOutScores.recipient_type,
    UnnormalizedMaxedOutScores.maxed_out_score
)


class MaxMaxedOutScore(BaseModel):
    max_maxed_out_score = FloatField()


class MaxedOutScores(UnnormalizedMaxedOutScores):
    pass

MaxedOutScores.add_index(
    MaxedOutScores.fec_committee_id,
    MaxedOutScores.other_id
)


class UnnormalizedLengthScores(BaseModel):
    fec_committee_id = CharField(max_length=9, null=False)
    contributor_name = CharField(max_length=200)
    other_id = CharField(max_length=9, null=False)
    recipient_name = CharField(max_length=200)
    max_date = DateTimeField()
    min_date = DateTimeField()
    length_score = FloatField()

UnnormalizedLengthScores.add_index(
    UnnormalizedLengthScores.fec_committee_id,
    UnnormalizedLengthScores.contributor_name,
    UnnormalizedLengthScores.other_id,
    UnnormalizedLengthScores.recipient_name,
    UnnormalizedLengthScores.max_date,
    UnnormalizedLengthScores.min_date,
    UnnormalizedLengthScores.length_score
)


class MaxLengthScore(BaseModel):
    max_length_score = FloatField()


class LengthScores(UnnormalizedLengthScores):
    pass

LengthScores.add_index(
    LengthScores.fec_committee_id,
    LengthScores.other_id
)


class RacesList(BaseModel):
    fec_committee_id = CharField(max_length=9, null=False)
    contributor_name = CharField(max_length=200)
    other_id = CharField(max_length=9, null=False)
    recipient_name = CharField(max_length=200)
    fec_candidate_id = CharField(max_length=9, null=False)
    candidate_name = CharField(max_length=200)
    district = CharField(max_length=3)
    office_state = CharField(max_length=3)
    branch = CharField(max_length=2)
    cycle = CharField(max_length=5)

    class Meta:
        indexes = (
            (('fec_committee_id', 'cycle', 'district', 'office_state', 'branch', 'contributor_name'), False),
        )


class RaceFocusScores(BaseModel):
    fec_committee_id = CharField(max_length = 9, null=False)
    contributor_name = CharField(max_length = 200)
    race_focus_score = FloatField(20)

    class Meta:
        indexes = (
            (('fec_committee_id', 'race_focus_score'), False),
        )


class ScoreWeights(BaseModel):
    score_type = CharField(max_length=30, null=False, index=True)
    weight = FloatField()


class FiveScores(BaseModel):
    fec_committee_id = CharField(max_length=9, null=False)
    contributor_name = CharField(max_length=200)
    other_id = CharField(max_length=9, null=False)
    recipient_name = CharField(max_length=200)
    exclusivity_score = FloatField()
    report_type_score = FloatField()
    periodicity_score = FloatField()
    maxed_out_score = FloatField()
    length_score = FloatField()
    five_score = FloatField()

    class Meta:
        indexes = (
            (('fec_committee_id', 'contributor_name', 'other_id', 'recipient_name', 'five_score'), False),
        )


class FinalScores(BaseModel):
    fec_committee_id = CharField(max_length=9, null=False)
    contributor_name = CharField(max_length=200)
    other_id = CharField(max_length=9, null=False)
    recipient_name = CharField(max_length=200)
    count = IntegerField()
    exclusivity_score = FloatField()
    report_type_score = FloatField()
    periodicity_score = FloatField()
    maxed_out_score = FloatField()
    length_score = FloatField()
    race_focus_score = FloatField()
    final_score = FloatField()

    class Meta:
        indexes = (
            (('fec_committee_id', 'other_id'), False),
        )


class FiveSum(BaseModel):
    fec_committee_id = CharField(max_length=9, null=False)
    contributor_name = CharField(max_length=200)
    other_id = CharField(max_length=9, null=False)
    recipient_name = CharField(max_length=200)
    exclusivity_score = FloatField()
    report_type_score = FloatField()
    periodicity_score = FloatField()
    maxed_out_score = FloatField()
    length_score = FloatField()
    five_sum = FloatField()

    class Meta:
        indexes = (
            (('fec_committee_id', 'contributor_name', 'other_id', 'recipient_name', 'five_sum'), False),
        )


class FinalSum(BaseModel):
    fec_committee_id = CharField(max_length=9, null=False)
    contributor_name = CharField(max_length=200)
    other_id = CharField(max_length=9, null=False)
    recipient_name = CharField(max_length=200)
    count = IntegerField()
    exclusivity_score = FloatField()
    report_type_score = FloatField()
    periodicity_score = FloatField()
    maxed_out_score = FloatField()
    length_score = FloatField()
    race_focus_score = FloatField()
    final_sum = FloatField()

    class Meta:
        indexes = (
            (('fec_committee_id', 'other_id'), False),
        )
