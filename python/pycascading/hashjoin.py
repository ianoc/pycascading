#
# Copyright 2011 Twitter, Inc.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Operations related to a HashJoin pipe."""

__author__ = 'Gabor Szabo/Ian O Connell'


import cascading.pipe
import cascading.operation
from pycascading.operators import rename
import cascading.pipe.assembly.Discard
from pycascading.pipe import Operation, coerce_to_fields, _Stackable, random_pipe_name


class HashJoin(Operation):

    """HashJoin two or more streams on common fields.

    This is a PyCascading wrapper around a Cascading HashJoin.
    """

    def __init__(self, *args, **kwargs):
        """Create a Cascading CoGroup pipe.

        Arguments:
        args[0] -- the fields on which to join

        Keyword arguments:
        group_name -- the groupName parameter for Cascading
        group_fields -- the fields on which to group
        declared_fields -- the declaredFields parameter for Cascading
        joiner -- the joiner parameter for Cascading
        num_self_joins -- the numSelfJoins parameter for Cascading
        """
        Operation.__init__(self)
        self.__args = args
        self.__kwargs = kwargs

    def __create_args(self,
                      pipes=None, declared_fields=None,
                      joiner=None,
                      pipe=None, num_self_joins=None,
                      name=None, group_fields=None):
        self.__to_discard_fields = []
        # We can use an unnamed parameter only for group_fields
        if self.__args:
            group_fields = [coerce_to_fields(f) for f in self.__args[0]]
        args = []
        if len(pipes) == 1:
            pipe = pipes[0]
            pipes = None

        if pipes:
            if group_fields:
                group_fields = [coerce_to_fields(f) for f in group_fields]
                new_group_fields = [group_fields[0]]
                # So we expect an array of arrays which contain the fields
                # as the group fields
                # If there are duplicates we need to do some rename magic

                primary_fields = group_fields[0]
                for indx in range(1, len(group_fields)):
                    current = group_fields[indx]
                    new_names = []
                    for indy in range(current.size()):
                        if declared_fields is None and primary_fields.get(indy) == current.get(indy):
                            new_field_name = "%s_DELETE_ME_%d" % (str(primary_fields.get(indy)), indx)
                            pipes[indx] |= rename(str(primary_fields.get(indy)), new_field_name)
                            new_names.append(new_field_name)
                            self.__to_discard_fields.append(new_field_name)
                        else:
                            new_names.append(str(current.get(indy)))
                    new_group_fields.append(coerce_to_fields(new_names))

                args.append([p.get_assembly() for p in pipes])
                args.append([coerce_to_fields(f) for f in new_group_fields])
                if declared_fields:
                    args.append(coerce_to_fields(declared_fields))
                else:
                    args.append(None)
                if joiner is None:
                    joiner = cascading.pipe.joiner.InnerJoin()
                args.append(joiner)
            else:
                args.append([p.get_assembly() for p in pipes])
        elif pipe:
            args.append(pipe.get_assembly())
            args.append(coerce_to_fields(group_fields))
            args.append(int(num_self_joins))
            if declared_fields:
                args.append(coerce_to_fields(declared_fields))
            if joiner:
                args.append(joiner)
        return args

    def _create_with_parent(self, parent):
        if isinstance(parent, _Stackable):
            args = self.__create_args(pipes=parent.stack, **self.__kwargs)
        else:
            args = self.__create_args(pipe=parent, **self.__kwargs)

        cogroup = cascading.pipe.HashJoin(*args)
        if len(self.__to_discard_fields) > 0:
            p = cascading.pipe.Pipe(random_pipe_name("HashJoin"), cogroup)
            return cascading.pipe.assembly.Discard(p, coerce_to_fields(self.__to_discard_fields))
        else:
            return cogroup


def inner_hash_join(*args, **kwargs):
    """Shortcut for an inner join."""
    kwargs['joiner'] = cascading.pipe.joiner.InnerJoin()
    if not 'declared_fields' in kwargs:
        kwargs['declared_fields'] = None
    return HashJoin(*args, **kwargs)


def outer_hash_join(*args, **kwargs):
    """Shortcut for an outer join."""
    kwargs['joiner'] = cascading.pipe.joiner.OuterJoin()
    if not 'declared_fields' in kwargs:
        kwargs['declared_fields'] = None
    return HashJoin(*args, **kwargs)


def left_outer_hash_join(*args, **kwargs):
    """Shortcut for a left outer join."""
    # The documentation says a Cascading RightJoin is a right inner join, but
    # that's not true, it's really an outer join as it should be.
    kwargs['joiner'] = cascading.pipe.joiner.LeftJoin()
    if not 'declared_fields' in kwargs:
        kwargs['declared_fields'] = None
    return HashJoin(*args, **kwargs)


def right_outer_hash_join(*args, **kwargs):
    """Shortcut for a right outer join."""
    kwargs['joiner'] = cascading.pipe.joiner.RightJoin()
    if not 'declared_fields' in kwargs:
        kwargs['declared_fields'] = None
    return HashJoin(*args, **kwargs)
