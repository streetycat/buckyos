import { Network, Zap, Cpu, Globe, Cloud, Server } from 'lucide-react'
import { StatusBadge } from '../shared/StatusBadge'
import { LongField } from '../shared/LongField'
import type { ProviderView } from '../../../../api/aicc_mgr'
import type { AuthStatus } from '../../../../api/aicc_mgr'

const providerIcons: Record<string, typeof Network> = {
  sn_router: Network,
  openai: Zap,
  anthropic: Cpu,
  google: Globe,
  openrouter: Cloud,
  custom: Server,
}

function authStatusToVariant(s: AuthStatus): 'ok' | 'warning' | 'error' | 'unknown' {
  switch (s) {
    case 'ok': return 'ok'
    case 'expired': return 'warning'
    case 'invalid': return 'error'
    default: return 'unknown'
  }
}

interface ProviderCardProps {
  provider: ProviderView
  selected: boolean
  onClick: () => void
  snAvailability?: SnProviderAvailability
}

export type SnProviderAvailability = {
  status: 'checking' | 'available' | 'unavailable' | 'unknown'
  reason?: string
}

export function ProviderCard({ provider, selected, onClick, snAvailability }: ProviderCardProps) {
  const Icon = providerIcons[provider.config.provider_type] ?? Server
  const modelCount = provider.status.discovered_models.length
  const degradedCount = provider.status.discovered_models.filter((m) => m.health.status !== 'available').length
  const isSnProvider = provider.config.provider_type === 'sn_router'
  const availabilityStatus = isSnProvider ? snAvailability?.status : undefined
  const badgeStatus =
    availabilityStatus === 'unavailable'
      ? 'error'
      : availabilityStatus === 'available'
        ? 'ok'
        : availabilityStatus === 'checking'
          ? 'unknown'
          : authStatusToVariant(provider.status.auth_status)
  const badgeLabel =
    availabilityStatus === 'unavailable'
      ? 'Auth failed'
      : availabilityStatus === 'available'
        ? '/models ok'
        : availabilityStatus === 'checking'
          ? 'Checking'
          : undefined
  const reasonColor =
    availabilityStatus === 'unavailable'
      ? 'var(--cp-danger)'
      : availabilityStatus === 'unknown'
        ? 'var(--cp-warning)'
        : 'var(--cp-muted)'

  return (
    <button
      type="button"
      onClick={onClick}
      className="flex min-h-16 w-full items-start gap-3 rounded-lg px-3 py-3 text-left transition-colors"
      style={{
        background: selected ? 'var(--cp-surface-2)' : 'transparent',
      }}
    >
      <Icon size={18} className="mt-0.5 shrink-0" style={{ color: 'var(--cp-muted)' }} />
      <div className="flex min-w-0 flex-1 flex-col gap-0.5">
        <LongField value={provider.config.name} className="text-sm font-medium" copyable={false} />
        <LongField
          value={`${provider.config.provider_instance_name}/${provider.config.provider_driver}`}
          className="text-[11px]"
          tone="muted"
          copyable={false}
        />
        {isSnProvider && snAvailability?.reason && availabilityStatus !== 'available' && (
          <span className="text-[11px] leading-4" style={{ color: reasonColor }}>
            {snAvailability.reason}
          </span>
        )}
      </div>
      <div className="flex shrink-0 flex-col items-end gap-1">
        <StatusBadge status={badgeStatus} label={badgeLabel} />
        <span className="text-[11px]" style={{ color: 'var(--cp-muted)' }}>
          {modelCount}{degradedCount > 0 ? `/${degradedCount}` : ''}
        </span>
      </div>
    </button>
  )
}
