"""Add out_zenodo_logs

Revision ID: f5dbba621bb1
Revises: b4fee22b4a4d
Create Date: 2024-10-25 13:30:27.163055

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'f5dbba621bb1'
down_revision: Union[str, None] = 'b4fee22b4a4d'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('out_zenodo_logs',
    sa.Column('metrics_date', sa.Date(), nullable=False, comment='The date when the metadata was reported.'),
    sa.Column('version', sa.String(), nullable=True, comment='The version (e.g. 10.0.0) of the dataset record.'),
    sa.Column('dataset_slug', sa.String(), nullable=True, comment='The shorthand for the dataset being archived. Matches the pudl_archiver repository dataset slugs when the dataset is archived by the PUDL archiver.'),
    sa.Column('new_dataset_downloads', sa.Integer(), nullable=True, comment='The number of new daily downloads for the entire dataset. A download is a user (human or machine) downloading a file from a record, excluding double-clicks and robots. If a record has multiple files and you download all files, each file counts as one download.'),
    sa.Column('new_dataset_unique_downloads', sa.Integer(), nullable=True, comment='The number of new daily unique downloads for the entire dataset. A unique download is defined as one or more file downloads from files of a single record by a user within a 1-hour time-window. This means that if one or more files of the same record were downloaded multiple times by the same user within the same time-window, it is considered to be one unique download.'),
    sa.Column('new_dataset_views', sa.Integer(), nullable=True, comment='The number of new daily views for the entire dataset. A total view is a user (human or machine) visiting a record, excluding double-clicks and robots.'),
    sa.Column('new_dataset_unique_views', sa.Integer(), nullable=True, comment='The number of new daily unique downloads for the entire dataset. A unique view is defined as one or more visits by a user within a 1-hour time-window. This means that if the same record was accessed multiple times by the same user within the same time-window, Zenodo considers it as one unique view.'),
    sa.Column('new_version_downloads', sa.Integer(), nullable=True, comment='The number of new daily downloads for the version. A total download is a user (human or machine) downloading a file from a record, excluding double-clicks and robots. If a record has multiple files and you download all files, each file counts as one download.'),
    sa.Column('new_version_unique_downloads', sa.Integer(), nullable=True, comment='The number of new daily unique downloads for the version. A unique download is defined as one or more file downloads from files of a single record by a user within a 1-hour time-window. This means that if one or more files of the same record were downloaded multiple times by the same user within the same time-window, it is considered to be one unique download.'),
    sa.Column('new_version_views', sa.Integer(), nullable=True, comment='The number of new daily views for the version. A total view is a user (human or machine) visiting a record, excluding double-clicks and robots.'),
    sa.Column('new_version_unique_views', sa.Integer(), nullable=True, comment='The number of new daily unique downloads for the version. A unique view is defined as one or more visits by a user within a 1-hour time-window. This means that if the same record was accessed multiple times by the same user within the same time-window, Zenodo considers it as one unique view.'),
    sa.Column('version_title', sa.String(), nullable=True, comment='The name of the version in Zenodo.'),
    sa.Column('version_id', sa.Integer(), nullable=False, comment='The unique ID of the Zenodo version. This is identical to the version DOI.'),
    sa.Column('version_record_id', sa.Integer(), nullable=True, comment='The record ID of the Zenodo version. This is identical to the version ID.'),
    sa.Column('concept_record_id', sa.Integer(), nullable=True, comment='The concept record ID. This is shared between all versions of a record.'),
    sa.Column('version_creation_date', sa.DateTime(), nullable=True, comment='The datetime the record was created.'),
    sa.Column('version_last_modified_date', sa.DateTime(), nullable=True, comment='The datetime the record was last modified.'),
    sa.Column('version_last_updated_date', sa.DateTime(), nullable=True, comment='The datetime the record was last updated.'),
    sa.Column('version_publication_date', sa.Date(), nullable=True, comment='The date that the version was published.'),
    sa.Column('version_doi', sa.String(), nullable=True, comment='The DOI of the Zenodo version.'),
    sa.Column('concept_record_doi', sa.String(), nullable=True, comment='The DOI of the Zenodo concept record.'),
    sa.Column('version_doi_url', sa.String(), nullable=True, comment='The DOI link of the Zenodo version.'),
    sa.Column('version_status', sa.String(), nullable=True, comment='The status of the Zenodo version.'),
    sa.Column('version_state', sa.String(), nullable=True, comment='The state of the Zenodo version.'),
    sa.Column('version_submitted', sa.Boolean(), nullable=True, comment='Is the version submitted?'),
    sa.Column('version_description', sa.String(), nullable=True, comment='The description of the version.'),
    sa.Column('partition_key', sa.String(), nullable=True),
    sa.PrimaryKeyConstraint('metrics_date', 'version_id')
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('out_zenodo_logs')
    # ### end Alembic commands ###
