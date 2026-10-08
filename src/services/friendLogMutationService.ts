import { useFriendLogStore } from '@/state/friendLogStore';
import { usePreferencesStore } from '@/state/preferencesStore';
import { useShellStore } from '@/state/shellStore';

export function signalFriendLogChanged({ notify = true } = {}) {
    useFriendLogStore.getState().bumpRevision();
    if (notify && usePreferencesStore.getState().friendLogNotificationDot) {
        useShellStore.getState().notifyMenu('friend-log');
    }
}
