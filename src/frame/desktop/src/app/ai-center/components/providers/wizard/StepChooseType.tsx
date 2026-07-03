import { Cloud, Cpu, Globe, Network, Zap, Server } from 'lucide-react'
import { useI18n } from '../../../../../i18n/provider'
import type { ProviderType } from '../../../../../api/aicc_mgr'

export type SnChooseAvailability = {
  status: 'checking' | 'available' | 'unavailable' | 'unknown'
  reason?: string
}

const PROVIDER_TYPES: {
  type: ProviderType
  name: string
  desc: string
  icon: typeof Network
  recommended?: boolean
}[] = [
  { type: 'sn_router', name: 'SN AI Provider', desc: 'Requires SN relay traffic mode and invite-code activation; uses local Device JWT', icon: Network, recommended: true },
  { type: 'openai', name: 'OpenAI', desc: 'GPT series models', icon: Zap },
  { type: 'anthropic', name: 'Anthropic', desc: 'Claude series models', icon: Cpu },
  { type: 'google', name: 'Google', desc: 'Gemini series models', icon: Globe },
  { type: 'openrouter', name: 'OpenRouter', desc: 'Multi-model aggregation router', icon: Cloud },
  { type: 'custom', name: 'Custom', desc: 'Custom API endpoint, supports OpenAI/Anthropic/Google protocol', icon: Server },
]

interface StepChooseTypeProps {
  selected: ProviderType | null
  onSelect: (type: ProviderType) => void
  snAvailability?: SnChooseAvailability
}

export function StepChooseType({ selected, onSelect, snAvailability }: StepChooseTypeProps) {
  const { t } = useI18n()

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
      {PROVIDER_TYPES.map((item) => {
        const active = selected === item.type
        const isSnProvider = item.type === 'sn_router'
        const snUnavailable = isSnProvider && snAvailability?.status === 'unavailable'
        const snChecking = isSnProvider && snAvailability?.status === 'checking'
        const snCheckWarning = isSnProvider && snAvailability?.status === 'unknown'
        const disabled = snUnavailable || snChecking
        const badge = isSnProvider && snUnavailable
          ? t('aiCenter.wizard.authFailed', 'Auth failed')
          : isSnProvider && snChecking
            ? t('common.checking', 'Checking')
            : isSnProvider && snCheckWarning
              ? t('aiCenter.wizard.checkWarning', 'Check warning')
              : item.recommended
                ? t('aiCenter.wizard.recommended', 'Recommended')
                : null
        const badgeBackground = snUnavailable
          ? 'var(--cp-danger)'
          : snChecking || snCheckWarning
            ? 'var(--cp-warning)'
            : 'var(--cp-accent)'
        return (
          <button
            key={item.type}
            type="button"
            onClick={() => {
              if (!disabled) onSelect(item.type)
            }}
            disabled={disabled}
            title={isSnProvider ? snAvailability?.reason : undefined}
            className="flex flex-col gap-2 p-4 rounded-xl text-left transition-all disabled:cursor-not-allowed"
            style={{
              background: active ? 'color-mix(in oklch, var(--cp-accent), transparent 90%)' : 'var(--cp-surface)',
              border: snUnavailable
                ? '1px solid color-mix(in srgb, var(--cp-danger) 55%, var(--cp-border))'
                : active ? '2px solid var(--cp-accent)' : '1px solid var(--cp-border)',
              opacity: disabled ? 0.78 : 1,
            }}
          >
            <div className="flex items-center gap-2">
              <item.icon
                size={20}
                style={{
                  color: snUnavailable
                    ? 'var(--cp-danger)'
                    : active ? 'var(--cp-accent)' : 'var(--cp-muted)',
                }}
              />
              <span className="text-sm font-medium" style={{ color: 'var(--cp-text)' }}>
                {item.name}
              </span>
              {badge && (
                <span
                  className="text-[10px] px-1.5 py-0.5 rounded font-medium"
                  style={{ background: badgeBackground, color: '#fff' }}
                >
                  {badge}
                </span>
              )}
            </div>
            <span className="text-xs" style={{ color: 'var(--cp-muted)' }}>
              {item.desc}
            </span>
            {isSnProvider && snAvailability?.reason && snAvailability.status !== 'available' && (
              <span
                className="text-[11px] leading-4"
                style={{
                  color: snUnavailable
                    ? 'var(--cp-danger)'
                    : snCheckWarning
                      ? 'var(--cp-warning)'
                      : 'var(--cp-muted)',
                }}
              >
                {snAvailability.reason}
              </span>
            )}
          </button>
        )
      })}
    </div>
  )
}
