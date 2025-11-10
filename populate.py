# Import necessary libraries
import os
import random
import time
import argparse
from datetime import timezone

import yaml
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
from faker import Faker

