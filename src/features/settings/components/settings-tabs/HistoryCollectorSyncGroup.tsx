import { RefreshCwIcon } from 'lucide-react';
import { useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';

import { commands, type HistorySyncStatus } from '@/platform/tauri/bindings';
import configRepository from '@/repositories/configRepository';
import { startHistoryCollectorSyncForCurrentSession } from '@/services/authenticatedRuntimeService';
import { signalFriendLogChanged } from '@/services/friendLogMutationService';
import { toast } from '@/services/toastService';
import { useRuntimeStore } from '@/state/runtimeStore';
import { Button } from '@/ui/shadcn/button';
import { Input } from '@/ui/shadcn/input';
import { Switch } from '@/ui/shadcn/switch';

import { SettingsCard } from '../SettingsCard';
import { Field } from '../SettingsField';

const SERVER_URL_KEY = 'HistorySyncServerUrl';
const TOKEN_KEY = 'HistorySyncToken';
const ENABLED_KEY = 'HistorySyncEnabled';

export function HistoryCollectorSyncGroup() {
    const { t } = useTranslation();
    const userId = useRuntimeStore(
        (state) => state.authenticatedSession.session?.userId ?? ''
    );
    const [serverUrl, setServerUrl] = useState('');
    const [token, setToken] = useState('');
    const [enabled, setEnabled] = useState(false);
    const [loading, setLoading] = useState(true);
    const [busy, setBusy] = useState(false);
    const [lastSync, setLastSync] = useState<HistorySyncStatus | null>(null);
    const [error, setError] = useState<string | null>(null);

    useEffect(() => {
        let active = true;
        Promise.all([
            configRepository.getString(SERVER_URL_KEY, ''),
            configRepository.getString(TOKEN_KEY, ''),
            configRepository.getBool(ENABLED_KEY, false)
        ])
            .then(([url, savedToken, savedEnabled]) => {
                if (active) {
                    setServerUrl(url);
                    setToken(savedToken);
                    setEnabled(savedEnabled);
                }
            })
            .catch((loadError: unknown) => {
                if (active) {
                    setError(String(loadError));
                }
            })
            .finally(() => {
                if (active) {
                    setLoading(false);
                }
            });
        return () => {
            active = false;
        };
    }, []);

    async function saveSettings() {
        setBusy(true);
        setError(null);
        try {
            await configRepository.setMany([
                [SERVER_URL_KEY, serverUrl.trim().replace(/\/+$/, '')],
                [TOKEN_KEY, token.trim()],
                [ENABLED_KEY, enabled ? 'true' : 'false']
            ]);
            setServerUrl((value) => value.trim().replace(/\/+$/, ''));
            if (enabled) {
                startHistoryCollectorSyncForCurrentSession();
            }
            toast.add({
                type: 'success',
                title: t('view.settings.integrations.history_collector.saved')
            });
        } catch (saveError: unknown) {
            setError(String(saveError));
        } finally {
            setBusy(false);
        }
    }

    async function syncNow() {
        const normalizedUrl = serverUrl.trim().replace(/\/+$/, '');
        const normalizedToken = token.trim();
        if (!normalizedUrl || !normalizedToken || !userId) {
            return;
        }
        setBusy(true);
        setError(null);
        try {
            await configRepository.setMany([
                [SERVER_URL_KEY, normalizedUrl],
                [TOKEN_KEY, normalizedToken],
                [ENABLED_KEY, enabled ? 'true' : 'false']
            ]);
            const status = await commands.appHistorySyncNow(
                normalizedUrl,
                normalizedToken,
                userId
            );
            setLastSync(status);
            if (status.imported > 0 || status.skippedDuplicates > 0) {
                signalFriendLogChanged({ notify: false });
            }
            if (enabled) {
                startHistoryCollectorSyncForCurrentSession();
            }
            toast.add({
                type: 'success',
                title: t(
                    'view.settings.integrations.history_collector.sync_complete',
                    { imported: status.imported }
                )
            });
        } catch (syncError: unknown) {
            const message =
                syncError instanceof Error
                    ? syncError.message
                    : String(syncError);
            setError(message);
            toast.add({ type: 'error', title: message });
        } finally {
            setBusy(false);
        }
    }

    const ready = Boolean(serverUrl.trim() && token.trim());

    return (
        <SettingsCard
            cardId="integrations.history-collector"
            title={t('view.settings.integrations.history_collector.header')}
            description={t(
                'view.settings.integrations.history_collector.description'
            )}
        >
            <Field
                label={t('view.settings.integrations.history_collector.enable')}
                description={t(
                    'view.settings.integrations.history_collector.enable_description'
                )}
            >
                <Switch
                    checked={enabled}
                    disabled={loading || busy}
                    onCheckedChange={setEnabled}
                />
            </Field>
            <Field
                label={t(
                    'view.settings.integrations.history_collector.server_url'
                )}
                description={t(
                    'view.settings.integrations.history_collector.server_url_description'
                )}
            >
                <Input
                    type="url"
                    value={serverUrl}
                    placeholder="https://collector.example.com"
                    autoComplete="url"
                    disabled={loading || busy}
                    onChange={(event) => setServerUrl(event.target.value)}
                />
            </Field>
            <Field
                label={t('view.settings.integrations.history_collector.token')}
                description={t(
                    'view.settings.integrations.history_collector.token_description'
                )}
            >
                <Input
                    type="password"
                    value={token}
                    autoComplete="new-password"
                    disabled={loading || busy}
                    onChange={(event) => setToken(event.target.value)}
                />
            </Field>
            <Field
                label={t('view.settings.integrations.history_collector.sync')}
                description={t(
                    'view.settings.integrations.history_collector.sync_description'
                )}
                error={error || undefined}
            >
                <div className="flex flex-wrap items-center justify-end gap-2">
                    {lastSync ? (
                        <span className="text-muted-foreground text-xs">
                            {t(
                                'view.settings.integrations.history_collector.last_sync',
                                {
                                    imported: lastSync.imported,
                                    pages: lastSync.pages
                                }
                            )}
                        </span>
                    ) : null}
                    <Button
                        type="button"
                        variant="outline"
                        size="sm"
                        disabled={loading || busy}
                        onClick={() => void saveSettings()}
                    >
                        {t('common.actions.save')}
                    </Button>
                    <Button
                        type="button"
                        size="sm"
                        disabled={loading || busy || !ready || !userId}
                        onClick={() => void syncNow()}
                    >
                        <RefreshCwIcon data-icon="inline-start" />
                        {t(
                            'view.settings.integrations.history_collector.sync_now'
                        )}
                    </Button>
                </div>
            </Field>
        </SettingsCard>
    );
}
