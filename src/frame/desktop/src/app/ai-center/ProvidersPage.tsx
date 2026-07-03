import { useEffect, useRef, useState } from 'react'
import { useMediaQuery } from '@mui/material'
import { useI18n } from '../../i18n/provider'
import { useAICCStore, useProviders, useGlobalRoutingView } from './hooks/use-aicc-store'
import { ProviderList } from './components/providers/ProviderList'
import { ProviderDetailPanel } from './components/providers/ProviderDetailPanel'
import { EmptyState } from './components/shared/EmptyState'
import { Plug } from 'lucide-react'
import type { AICenterPage } from './components/layout/Sidebar'
import type { SnProviderAvailability } from './components/providers/ProviderCard'
import type { ProviderView, ValidationResult, WizardDraft } from '../../api/aicc_mgr'

interface ProvidersPageProps {
  navigate: (page: AICenterPage) => void
}

export function ProvidersPage({ navigate }: ProvidersPageProps) {
  const { t } = useI18n()
  const store = useAICCStore()
  const providers = useProviders()
  const routingView = useGlobalRoutingView()
  const isMobile = useMediaQuery('(max-width: 767px)')
  const isCompactDesktop = useMediaQuery('(min-width: 768px) and (max-width: 1100px)')
  const [selectedId, setSelectedId] = useState<string | null>(
    providers.length > 0 ? providers[0].config.id : null,
  )
  // Mobile: detail view shown when a provider is selected and user tapped it
  const [showMobileDetail, setShowMobileDetail] = useState(false)
  const mobileListRef = useRef<HTMLDivElement | null>(null)
  const mobileListScrollTop = useRef(0)
  const [snAvailabilityById, setSnAvailabilityById] = useState<Record<string, SnProviderAvailability>>({})

  const selectedProvider = providers.find((p) => p.config.id === selectedId)

  useEffect(() => {
    const snProviders = providers.filter((provider) => provider.config.provider_type === 'sn_router')
    if (snProviders.length === 0) {
      setSnAvailabilityById({})
      return
    }

    let cancelled = false
    setSnAvailabilityById((current) => {
      const next: Record<string, SnProviderAvailability> = {}
      for (const provider of snProviders) {
        next[provider.config.id] = current[provider.config.id] ?? {
          status: 'checking',
          reason: t('aiCenter.providers.snCheckingModels', 'Checking SN /models permission.'),
        }
      }
      return next
    })

    snProviders.forEach((provider) => {
      void store.validateConnection(snProviderValidationDraft(provider)).then((result) => {
        if (cancelled) return
        setSnAvailabilityById((current) => ({
          ...current,
          [provider.config.id]: snAvailabilityFromValidation(result, t),
        }))
      }).catch((error) => {
        if (cancelled) return
        setSnAvailabilityById((current) => ({
          ...current,
          [provider.config.id]: {
            status: 'unknown',
            reason: error instanceof Error
              ? error.message
              : t('aiCenter.providers.snCheckFailed', 'SN /models availability check failed.'),
          },
        }))
      })
    })

    return () => {
      cancelled = true
    }
  }, [providers, store, t])

  useEffect(() => {
    if (!isMobile || showMobileDetail) return
    const node = mobileListRef.current
    if (!node) return
    node.scrollTop = mobileListScrollTop.current
  }, [isMobile, showMobileDetail])

  if (providers.length === 0) {
    return (
      <EmptyState
        icon={<Plug size={48} />}
        title={t('aiCenter.providers.noProviders', 'No providers configured')}
        action={{
          label: t('aiCenter.providers.addProvider', 'Add Provider'),
          onClick: () => navigate('providers/add'),
        }}
      />
    )
  }

  if (isMobile) {
    if (showMobileDetail && selectedProvider) {
      return (
        <div>
          <ProviderDetailPanel
            provider={selectedProvider}
            routingWeight={routingView.provider_weights[selectedProvider.config.provider_instance_name] ?? 1}
            onBack={() => setShowMobileDetail(false)}
            onDeleted={() => {
              setShowMobileDetail(false)
              setSelectedId(providers.length > 1 ? providers[0].config.id : null)
            }}
          />
        </div>
      )
    }

    return (
      <div ref={mobileListRef} className="max-h-full overflow-y-auto pb-[calc(1rem+env(safe-area-inset-bottom))]">
        <ProviderList
          providers={providers}
          selectedId={selectedId}
          onSelect={(id) => {
            mobileListScrollTop.current = mobileListRef.current?.scrollTop ?? 0
            setSelectedId(id)
            setShowMobileDetail(true)
          }}
          onAdd={() => navigate('providers/add')}
          snAvailabilityById={snAvailabilityById}
        />
      </div>
    )
  }

  // Desktop: split view
  return (
    <div className={`${isCompactDesktop ? 'flex flex-col' : 'flex'} gap-6 -mx-8 -my-6 h-full`}>
      <div
        className={isCompactDesktop ? 'max-h-72 shrink-0 overflow-y-auto px-4 py-4' : 'w-80 shrink-0 overflow-y-auto px-4 py-4'}
        style={isCompactDesktop ? { borderBottom: '1px solid var(--cp-border)' } : { borderRight: '1px solid var(--cp-border)' }}
      >
        <ProviderList
          providers={providers}
          selectedId={selectedId}
          onSelect={setSelectedId}
          onAdd={() => navigate('providers/add')}
          snAvailabilityById={snAvailabilityById}
        />
      </div>
      <div className="flex-1 py-6 px-6 overflow-y-auto">
        {selectedProvider ? (
          <ProviderDetailPanel
            provider={selectedProvider}
            routingWeight={routingView.provider_weights[selectedProvider.config.provider_instance_name] ?? 1}
            onDeleted={() => {
              const remaining = providers.filter((p) => p.config.id !== selectedId)
              setSelectedId(remaining.length > 0 ? remaining[0].config.id : null)
            }}
          />
        ) : (
          <div className="flex items-center justify-center h-full text-sm" style={{ color: 'var(--cp-muted)' }}>
            {t('aiCenter.providers.detail', 'Provider Detail')}
          </div>
        )}
      </div>
    </div>
  )
}

function snProviderValidationDraft(provider: ProviderView): WizardDraft {
  return {
    provider_instance_name: provider.config.provider_instance_name,
    provider_type: 'sn_router',
    name: provider.config.name,
    endpoint: provider.config.endpoint ?? '',
    protocol_type: null,
    api_key: '',
    auto_sync_models: provider.config.auto_sync_models,
  }
}

function validationReason(result: ValidationResult, kind: 'endpoint' | 'auth' | 'models'): string | undefined {
  return result.error_details?.find((item) => item.kind === kind)?.message
}

function snAvailabilityFromValidation(
  result: ValidationResult,
  t: ReturnType<typeof useI18n>['t'],
): SnProviderAvailability {
  if (!result.auth_valid) {
    return {
      status: 'unavailable',
      reason: validationReason(result, 'auth')
        ?? result.errors[0]
        ?? t('aiCenter.providers.snAuthUnavailable', 'SN /models permission denied. SN relay traffic mode and invite-code activation are required.'),
    }
  }
  if (!result.endpoint_reachable || result.errors.length > 0) {
    return {
      status: 'unknown',
      reason: validationReason(result, 'endpoint')
        ?? validationReason(result, 'models')
        ?? result.errors[0]
        ?? t('aiCenter.providers.snCheckFailed', 'SN /models availability check failed.'),
    }
  }
  return {
    status: 'available',
    reason: t('aiCenter.providers.snModelsAvailable', '{{count}} models listed by /models.', {
      count: result.models_discovered.length,
    }),
  }
}
