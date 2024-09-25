"""Create zenodo log table

Revision ID: 68374d2f6f62
Revises: e8435a653eb2
Create Date: 2024-09-25 12:10:16.201331

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '68374d2f6f62'
down_revision: Union[str, None] = 'e8435a653eb2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('core_zenodo_logs',
    sa.Column('metrics_date', sa.Date(), nullable=False, comment='The date when the metadata was reported.'),
    sa.Column('version_doi', sa.String(), nullable=False, comment='The DOI of the Zenodo version.'),
    sa.Column('dataset_downloads', sa.Integer(), nullable=True, comment='The total number of downloads for the entire dataset. A total download is a user (human or machine) downloading a file from a record, excluding double-clicks and robots. If a record has multiple files and you download all files, each file counts as one download.'),
    sa.Column('dataset_unique_downloads', sa.Integer(), nullable=True, comment='The total number of unique downloads for the entire dataset. A unique download is defined as one or more file downloads from files of a single record by a user within a 1-hour time-window. This means that if one or more files of the same record were downloaded multiple times by the same user within the same time-window, it is considered to be one unique download.'),
    sa.Column('dataset_views', sa.Integer(), nullable=True, comment='The total number of views for the entire dataset. A total view is a user (human or machine) visiting a record, excluding double-clicks and robots.'),
    sa.Column('dataset_unique_views', sa.Integer(), nullable=True, comment='The total number of unique downloads for the entire dataset. A unique view is defined as one or more visits by a user within a 1-hour time-window. This means that if the same record was accessed multiple times by the same user within the same time-window, Zenodo considers it as one unique view.'),
    sa.Column('version_downloads', sa.Integer(), nullable=True, comment='The total number of downloads for the version. A total download is a user (human or machine) downloading a file from a record, excluding double-clicks and robots. If a record has multiple files and you download all files, each file counts as one download.'),
    sa.Column('version_unique_downloads', sa.Integer(), nullable=True, comment='The total number of unique downloads for the version. A unique download is defined as one or more file downloads from files of a single record by a user within a 1-hour time-window. This means that if one or more files of the same record were downloaded multiple times by the same user within the same time-window, it is considered to be one unique download.'),
    sa.Column('version_views', sa.Integer(), nullable=True, comment='The total number of views for the version. A total view is a user (human or machine) visiting a record, excluding double-clicks and robots.'),
    sa.Column('version_unique_views', sa.Integer(), nullable=True, comment='The total number of unique downloads for the version. A unique view is defined as one or more visits by a user within a 1-hour time-window. This means that if the same record was accessed multiple times by the same user within the same time-window, Zenodo considers it as one unique view.'),
    sa.Column('version_title', sa.String(), nullable=True, comment='The name of the version in Zenodo.'),
    sa.Column('version', sa.String(), nullable=True, comment='The version (e.g. 10.0.0) of the dataset record.'),
    sa.Column('publication_date', sa.Date(), nullable=True, comment='The date that the version was published.'),
    sa.Column('dataset_slug', sa.String(), nullable=True, comment='The shorthand for the dataset being archived. Matches the pudl_archiver repository dataset slugs when the dataset is archived by the PUDL archiver.'),
    sa.Column('partition_key', sa.String(), nullable=True),
    sa.PrimaryKeyConstraint('metrics_date', 'version_doi')
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('core_zenodo_logs')
    # ### end Alembic commands ###
