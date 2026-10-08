import { commands } from '@/platform/tauri/bindings';
import configRepository from '@/repositories/configRepository';
import { useRuntimeStore } from '@/state/runtimeStore';
import { useSessionStore } from '@/state/sessionStore';

import { signalFriendLogChanged } from './friendLogMutationService';

const SERVER_URL_KEY = 'HistorySyncServerUrl';
const TOKEN_KEY = 'HistorySyncToken';
const ENABLED_KEY = 'HistorySyncEnabled';
const RETRY_DELAY_MS = 5 * 60 * 1000;

let lastAutomaticSyncKey = '';
const inFlightByUser = new Map<string, Promise<void>>();
const retryTimersByUser = new Map<string, ReturnType<typeof setTimeout>>();
const activeRunIdByUser = new Map<string, number>();

function clearRetryTimer(userId: string): void {
    const timer = retryTimersByUser.get(userId);
    if (timer !== undefined) {
        clearTimeout(timer);
        retryTimersByUser.delete(userId);
    }
}

function scheduleRetry(userId: string, runId: number): void {
    clearRetryTimer(userId);
    const timer = setTimeout(() => {
        retryTimersByUser.delete(userId);
        if (
            !useSessionStore.getState().isLoggedIn ||
            useRuntimeStore.getState().auth.currentUserId !== userId ||
            activeRunIdByUser.get(userId) !== runId
        ) {
            return;
        }
        lastAutomaticSyncKey = '';
        void syncCollectorHistoryForSession(userId, runId).catch(() => {
            // Failed requests are retried after the same delay.
        });
    }, RETRY_DELAY_MS);
    retryTimersByUser.set(userId, timer);
}

export async function syncCollectorHistoryForSession(
    userId: string,
    runId: number
): Promise<void> {
    const normalizedUserId = userId.trim();
    const syncKey = `${normalizedUserId}:${runId}`;
    if (!normalizedUserId || lastAutomaticSyncKey === syncKey) {
        return;
    }
    activeRunIdByUser.set(normalizedUserId, runId);
    clearRetryTimer(normalizedUserId);
    const existingSync = inFlightByUser.get(normalizedUserId);
    if (existingSync) {
        try {
            await existingSync;
        } catch {
            // A newer runtime can retry after the prior runtime's request settles.
        }
        if (lastAutomaticSyncKey !== syncKey) {
            await syncCollectorHistoryForSession(normalizedUserId, runId);
        }
        return;
    }

    const syncTask = (async () => {
        const [enabled, serverUrl, token] = await Promise.all([
            configRepository.getBool(ENABLED_KEY, false),
            configRepository.getString(SERVER_URL_KEY, ''),
            configRepository.getString(TOKEN_KEY, '')
        ]);
        const normalizedUrl = serverUrl.trim().replace(/\/+$/, '');
        const normalizedToken = token.trim();
        if (!enabled || !normalizedUrl || !normalizedToken) {
            return;
        }

        const result = await commands.appHistorySyncNow(
            normalizedUrl,
            normalizedToken,
            normalizedUserId
        );
        if (activeRunIdByUser.get(normalizedUserId) !== runId) {
            return;
        }
        if (result.imported > 0 || result.skippedDuplicates > 0) {
            signalFriendLogChanged({ notify: false });
        }
        lastAutomaticSyncKey = syncKey;
        scheduleRetry(normalizedUserId, runId);
    })();
    inFlightByUser.set(normalizedUserId, syncTask);
    try {
        await syncTask;
    } catch (error) {
        if (activeRunIdByUser.get(normalizedUserId) === runId) {
            scheduleRetry(normalizedUserId, runId);
        }
        throw error;
    } finally {
        if (inFlightByUser.get(normalizedUserId) === syncTask) {
            inFlightByUser.delete(normalizedUserId);
        }
    }
}

export function resetHistoryCollectorSyncStateForTests(): void {
    stopHistoryCollectorAutoSync();
}

export function stopHistoryCollectorAutoSync(): void {
    lastAutomaticSyncKey = '';
    inFlightByUser.clear();
    activeRunIdByUser.clear();
    for (const timer of retryTimersByUser.values()) {
        clearTimeout(timer);
    }
    retryTimersByUser.clear();
}
