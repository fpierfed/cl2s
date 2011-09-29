#!/usr/bin/env python
import logging
import sys
import unittest

import elixir

from cl2s.Job import Job
from cl2s import JobQueue
from cl2s import logutils



CLASS_AD = u'PeriodicRemove = false\nCommittedSlotTime = 0\nOut = "calacs_j9am01070.out"\nHookKeyword = "OWL"\nEnteredCurrentStatus = 1316535773\nCommittedSuspensionTime = 0\nWhenToTransferOutput = "ON_EXIT"\nNumSystemHolds = 0\nStreamOut = false\nNumRestarts = 0\nImageSize = 0\nCmd = "/jwst/bcw/python/acs_simple/bin/run_calacs.py"\nCurrentHosts = 0\nIwd = "/jwst/data/work/fpierfed_1316469957.377134"\nCumulativeSlotTime = 0\nCondorVersion = "$CondorVersion: 7.6.3 Aug 17 2011 BuildID: 361356 $"\nRemoteUserCpu = 0.0\nNumCkpts = 0\nArguments = "j9am01070_asn.fits"\nJobStatus = 1\nRemoteSysCpu = 0.0\nInstances = 4\nOnExitRemove = true\nBufferBlockSize = 32768\nIn = "/dev/null"\nLocalUserCpu = 0.0\nMinHosts = 1\nEnvironment = "iraf=/jwst/iraf/iraf/ PATH=/jwst/bin:/usr/kerberos/bin:/usr/local/bin:/bin:/usr/bin:/home/fpierfed/bin LD_LIBRARY_PATH=/jwst/lib: MAIL=/var/spool/mail/fpierfed CVS_RSH=ssh PS1=%m>\' \' LANG=en_US.UTF-8 SSH_CONNECTION=192.168.33.1\' \'50336\' \'192.168.33.128\' \'22 mtab=/jwst/cdbs/mtab/ SSH_AUTH_SOCK=/tmp/ssh-knqnOh4033/agent.4033 SSH_CLIENT=192.168.33.1\' \'50336\' \'22 SHELL=/bin/zsh LS_COLORS=no=00:fi=00:di=01;34:ln=01;36:pi=40;33:so=01;35:bd=40;33;01:cd=40;33;01:or=01;05;37;41:mi=01;05;37;41:ex=01;32:*.cmd=01;32:*.exe=01;32:*.com=01;32:*.btm=01;32:*.bat=01;32:*.sh=01;32:*.csh=01;32:*.tar=01;31:*.tgz=01;31:*.arj=01;31:*.taz=01;31:*.lzh=01;31:*.zip=01;31:*.z=01;31:*.Z=01;31:*.gz=01;31:*.bz2=01;31:*.bz=01;31:*.tz=01;31:*.rpm=01;31:*.cpio=01;31:*.jpg=01;35:*.gif=01;35:*.bmp=01;35:*.xbm=01;35:*.xpm=01;35:*.png=01;35:*.tif=01;35: _=/usr/bin/condor_submit PWD=/jwst/data/work/fpierfed_1316469957.377134 jref=/jwst/cdbs/jref/ SSH_TTY=/dev/pts/1 SAVEHIST=2000 SSH_ASKPASS=/usr/libexec/openssh/gnome-ssh-askpass cracscomp=/jwst/cdbs/acs/ HISTSIZE=2000 USER=fpierfed G_BROKEN_FILENAMES=1 LESSOPEN=|/usr/bin/lesspipe.sh\' \'%s HISTFILE=/home/fpierfed/.history SHLVL=1 HOSTNAME=centosvm1.localdomain IRAFARCH=redhat HOME=/home/fpierfed DRMAA_LIBRARY_PATH=/jwst/lib/libdrmaa.so TERM=dtterm crotacomp=/jwst/cdbs/ota/ INPUTRC=/etc/inputrc OLDPWD=/home/fpierfed EDITOR=vim LOGNAME=fpierfed"\nJobUniverse = 5\nRequestDisk = DiskUsage\nRootDir = "/"\nNumJobStarts = 0\nWantRemoteIO = true\nRequestMemory = ceiling(ifThenElse(JobVMMemory =!= undefined,JobVMMemory,ImageSize / 1024.000000))\nLocalSysCpu = 0.0\nPeriodicRelease = false\nDiskUsage = 0\nCumulativeSuspensionTime = 0\nJobLeaseDuration = 1200\nTransferInput = "j9am01070_asn.fits,j9am01ejq_raw.fits,j9am01egq_raw.fits,j9am01edq_raw.fits,j9am01emq_raw.fits"\nUserLog = "/jwst/data/work/fpierfed_1316469957.377134/j9am01070.log"\nExecutableSize = 0\nMaxHosts = 1\nCoreSize = 0\nTransferFiles = "ONEXIT"\nShouldTransferFiles = "YES"\nCommittedTime = 0\nTotalSuspensions = 0\nErr = "calacs_j9am01070.err"\nInputDataset = "j9am01070"\nRequestCpus = 1\nStreamErr = false\nNiceUser = false\nRemoteWallClockTime = 0.0\nTargetType = "Machine"\nPeriodicHold = false\nQDate = 1316535773\nOnExitHold = false\nRank = 0.0\nExitBySignal = false\nCondorPlatform = "$CondorPlatform: x86_64_rhap_5 $"\nJobPrio = 0\nLastSuspensionTime = 0\nJobNotification = 0\nCurrentTime = time()\nBufferSize = 524288\nWantRemoteSyscalls = false\nLeaveJobInQueue = false\nExitStatus = 0\nCompletionDate = 0\nRequirements = ( TARGET.Arch == "X86_64" ) && ( TARGET.OpSys == "LINUX" ) && ( TARGET.Disk >= DiskUsage ) && ( ( TARGET.Memory * 1024 ) >= ImageSize ) && ( ( RequestMemory * 1024 ) >= ImageSize ) && ( TARGET.HasFileTransfer )\nMyType = "Job"\nWantCheckpoint = false\nOwner = "fpierfed"\nTransferIn = false\n'
N = 1000


logutils.logger.setLevel(logging.WARNING)


class TestJobQueue(unittest.TestCase):
    def test_push(self):
        n = 0
        for i in range(N):
            j = Job(CLASS_AD.replace('j9am01070', 'j9am0%04d' % (i)))
            JobQueue.push(j)
            n += 1
        self.assertEqual(JobQueue.length(), n)
        return
    
    def test_length(self):
        # TODO: find a vendor agnostic way to test this.
        return
    
    def test_pop_and_delete(self):
        for i in range(N):
            j = JobQueue.pop()
            self.assertIsInstance(j, Job)
            JobQueue.delete(j)
        self.assertEqual(JobQueue.length(), 0)
        return
    



if(__name__ == '__main__'):
    print('WARNING: this wipes the Job Queue database.')
    answer = raw_input('Are you sure you want to continue? [yes/N] ')
    if(answer != 'yes'):
        print('Test aborted. The database has NOT been touched.')
        sys.exit(0)
    else:
        print('Wiping the database')
        # WIPE the queue!!!!!!
        elixir.setup_all()
        
        for jq in JobQueue.JobQueueEntry.query.all():
            jq.delete()
        elixir.session.commit()
        
        print('Done. Now running the tests.')
        test_suite = unittest.TestSuite()
        test_suite.addTest(TestJobQueue('test_push'))
        test_suite.addTest(TestJobQueue('test_length'))
        test_suite.addTest(TestJobQueue('test_pop_and_delete'))
        unittest.TextTestRunner(verbosity=2).run(test_suite)
    



































